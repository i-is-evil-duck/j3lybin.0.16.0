const std = @import("std");
const Io = std.Io;
const net = std.Io.net;

const MAX_FILE_SIZE: u64 = 25 * 1024 * 1024 * 1024; // 25GB
const MAX_DAILY_PER_IP: u64 = 25 * 1024 * 1024 * 1024; // 25GB
const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8MB chunks
const MAX_CHUNK_SIZE: usize = CHUNK_SIZE + 1024; // write-side cap for one chunk
const MAX_CHUNKS: u32 = @intCast((MAX_FILE_SIZE + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE);
const DEFAULT_TTL: u64 = 48 * 3600;
const MAX_TTL: u64 = 180 * 86400; // 6 months; must fit in i64 for expiry math
const DATA_DIR = "data";
const META_DIR = "data/meta";

fn nowSecs(io: Io) i64 {
    return @intCast(@divTrunc(Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_s));
}

const UploadMeta = struct {
    filename: []u8,
    total_size: u64,
    total_chunks: u32,
    ttl_seconds: u64,
    created_at: i64,
    chunks_received: std.DynamicBitSet,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, io: Io, filename: []const u8, total_size: u64, total_chunks: u32, ttl_seconds: u64) !UploadMeta {
        const filename_copy = try allocator.dupe(u8, filename);
        errdefer allocator.free(filename_copy);
        var chunks = try std.DynamicBitSet.initEmpty(allocator, total_chunks);
        errdefer chunks.deinit();
        return UploadMeta{
            .filename = filename_copy,
            .total_size = total_size,
            .total_chunks = total_chunks,
            .ttl_seconds = ttl_seconds,
            .created_at = nowSecs(io),
            .chunks_received = chunks,
            .allocator = allocator,
        };
    }

    fn deinit(self: *UploadMeta) void {
        self.allocator.free(self.filename);
        self.chunks_received.deinit();
    }

    fn isComplete(self: *const UploadMeta) bool {
        return self.chunks_received.count() == self.total_chunks;
    }
};

const SpinLock = struct {
    state: std.atomic.Mutex = .unlocked,

    fn lock(self: *SpinLock) void {
        while (!self.state.tryLock()) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlock(self: *SpinLock) void {
        self.state.unlock();
    }
};

const IpQuota = struct {
    bytes_used: u64,
    reset_at: i64,
};

const ServerState = struct {
    allocator: std.mem.Allocator,
    uploads: std.StringHashMap(*UploadMeta),
    ip_quotas: std.StringHashMap(IpQuota),
    mutex: SpinLock,
    ip_mutex: SpinLock,

    fn init(allocator: std.mem.Allocator, io: Io) !ServerState {
        const cwd = std.Io.Dir.cwd();
        try cwd.createDirPath(io, DATA_DIR);
        try cwd.createDirPath(io, META_DIR);
        return ServerState{
            .allocator = allocator,
            .uploads = std.StringHashMap(*UploadMeta).init(allocator),
            .ip_quotas = std.StringHashMap(IpQuota).init(allocator),
            .mutex = .{},
            .ip_mutex = .{},
        };
    }

    fn deinit(self: *ServerState) void {
        var it = self.uploads.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.uploads.deinit();

        var qit = self.ip_quotas.iterator();
        while (qit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.ip_quotas.deinit();
    }
};

var state: ServerState = undefined;

// Builds a per-IP quota key. Must NOT include the source port, otherwise each
// new TCP connection gets its own quota bucket and the daily limit never
// triggers. `{any}`/`{f}` on an IpAddress include the port, so we format the
// address bytes directly.
fn getClientIP(addr: net.IpAddress) ![]u8 {
    return switch (addr) {
        .ip4 => |ip4| {
            const b = ip4.bytes;
            return std.fmt.allocPrint(state.allocator, "{d}.{d}.{d}.{d}", .{ b[0], b[1], b[2], b[3] });
        },
        .ip6 => |ip6| {
            const b = ip6.bytes;
            // Group IPv4-mapped IPv6 addresses with their IPv4 peers.
            if (std.mem.eql(u8, b[0..10], &[_]u8{0} ** 10) and b[10] == 0xff and b[11] == 0xff) {
                return std.fmt.allocPrint(state.allocator, "{d}.{d}.{d}.{d}", .{ b[12], b[13], b[14], b[15] });
            }
            const value = std.mem.readInt(u128, b[0..16], .big);
            return std.fmt.allocPrint(state.allocator, "{x}", .{value});
        },
    };
}

fn checkIPQuota(io: Io, ip: []const u8, file_size: u64) !bool {
    state.ip_mutex.lock();
    defer state.ip_mutex.unlock();

    const now = nowSecs(io);
    const gop = try state.ip_quotas.getOrPut(ip);
    if (!gop.found_existing) {
        // Same ownership issue as uploads: `ip` is request-owned, so copy it.
        gop.key_ptr.* = try state.allocator.dupe(u8, ip);
        gop.value_ptr.* = .{ .bytes_used = 0, .reset_at = now + 86400 };
    }

    if (now > gop.value_ptr.reset_at) {
        gop.value_ptr.bytes_used = 0;
        gop.value_ptr.reset_at = now + 86400;
    }

    if (gop.value_ptr.bytes_used + file_size > MAX_DAILY_PER_IP) {
        return false;
    }
    return true;
}

fn addIPQuota(ip: []const u8, bytes: u64) !void {
    state.ip_mutex.lock();
    defer state.ip_mutex.unlock();

    if (state.ip_quotas.getPtr(ip)) |quota| {
        quota.bytes_used += bytes;
    }
}

fn parseTTL(ttl_str: []const u8) u64 {
    if (ttl_str.len < 2) return DEFAULT_TTL;
    const num = std.fmt.parseInt(u64, ttl_str[0 .. ttl_str.len - 1], 10) catch return DEFAULT_TTL;
    const unit = ttl_str[ttl_str.len - 1];
    const multiplier: u64 = switch (unit) {
        'm' => 60,
        'h' => 3600,
        'w' => 7 * 86400,
        'M' => 30 * 86400,
        else => return DEFAULT_TTL,
    };
    // Clamp instead of silently wrapping in ReleaseFast, and keep the value in
    // range for the i64 casts used by the expiry checks.
    const ttl = std.math.mul(u64, num, multiplier) catch std.math.maxInt(u64);
    return @min(ttl, MAX_TTL);
}

const MetaSnapshot = struct {
    filename: []const u8,
    total_size: u64,
    total_chunks: u32,
    ttl_seconds: u64,
    created_at: i64,
};

fn saveMeta(io: Io, upload_id: []const u8, meta: MetaSnapshot) !void {
    const path = try std.fmt.allocPrint(state.allocator, "{s}/{s}.json", .{ META_DIR, upload_id });
    defer state.allocator.free(path);

    const cwd = std.Io.Dir.cwd();
    var file = try cwd.createFile(io, path, .{});
    defer file.close(io);

    var buf: [4096]u8 = undefined;
    var w = file.writer(io, &buf);

    try w.interface.print("{{\n", .{});
    try w.interface.print("  \"filename\": \"{s}\",\n", .{meta.filename});
    try w.interface.print("  \"total_size\": {d},\n", .{meta.total_size});
    try w.interface.print("  \"total_chunks\": {d},\n", .{meta.total_chunks});
    try w.interface.print("  \"ttl_seconds\": {d},\n", .{meta.ttl_seconds});
    try w.interface.print("  \"created_at\": {d}\n", .{meta.created_at});
    try w.interface.print("}}\n", .{});
    try w.flush();
}

fn sendResponse(writer: *std.Io.Writer, status: u16, content_type: []const u8, body: []const u8, connection: []const u8) !void {
    const status_text = switch (status) {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        else => "Unknown",
    };
    const header = try std.fmt.allocPrint(state.allocator,
        "HTTP/1.1 {d} {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: {s}\r\n\r\n",
        .{ status, status_text, content_type, body.len, connection });
    defer state.allocator.free(header);
    try writer.writeAll(header);
    try writer.writeAll(body);
}

fn sendFile(io: Io, writer: *std.Io.Writer, file_path: []const u8, content_type: []const u8, connection: []const u8) !void {
    const cwd = std.Io.Dir.cwd();
    const stat = cwd.statFile(io, file_path, .{}) catch {
        try sendResponse(writer, 404, "text/plain", "Not found", connection);
        return;
    };

    var file = try cwd.openFile(io, file_path, .{});
    defer file.close(io);

    const header = try std.fmt.allocPrint(state.allocator,
        "HTTP/1.1 200 OK\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: {s}\r\n\r\n",
        .{ content_type, stat.size, connection });
    defer state.allocator.free(header);
    try writer.writeAll(header);

    var buf: [65536]u8 = undefined;
    var offset: u64 = 0;
    while (true) {
        const n = try file.readPositional(io, &[_][]u8{buf[0..]}, offset);
        if (n == 0) break;
        try writer.writeAll(buf[0..n]);
        offset += n;
    }
}

// Extracts the numeric value of `"<name>: "` from the JSON metadata written by
// saveMeta. Returns null if the field is missing or not an integer.
fn jsonFieldInt(data: []const u8, comptime name: []const u8) ?i64 {
    const needle = "\"" ++ name ++ "\": ";
    const start = std.mem.indexOf(u8, data, needle) orelse return null;
    var end = start + needle.len;
    while (end < data.len and data[end] >= '0' and data[end] <= '9') : (end += 1) {}
    if (end == start + needle.len) return null;
    return std.fmt.parseInt(i64, data[start + needle.len .. end], 10) catch null;
}

fn sendDownload(io: Io, writer: *std.Io.Writer, upload_id: []const u8, connection: []const u8) !void {
    const cwd = std.Io.Dir.cwd();
    const meta_path = try std.fmt.allocPrint(state.allocator, "{s}/{s}.json", .{ META_DIR, upload_id });
    defer state.allocator.free(meta_path);

    const meta_data = cwd.readFileAlloc(io, meta_path, state.allocator, .limited(4096)) catch {
        try sendResponse(writer, 404, "text/plain", "Not found", connection);
        return;
    };
    defer state.allocator.free(meta_data);

    // Parse filename from JSON
    var filename: []const u8 = "file";
    if (std.mem.indexOf(u8, meta_data, "\"filename\": \"")) |start| {
        const name_start = start + 13;
        if (std.mem.indexOf(u8, meta_data[name_start..], "\"")) |end| {
            filename = meta_data[name_start .. name_start + end];
        }
    }

    // Enforce TTL even before the cleanup thread runs.
    const created_at = jsonFieldInt(meta_data, "created_at") orelse 0;
    const ttl_seconds = jsonFieldInt(meta_data, "ttl_seconds") orelse 0;
    if (nowSecs(io) > created_at + @as(i64, @intCast(ttl_seconds))) {
        try sendResponse(writer, 404, "text/plain", "Expired", connection);
        return;
    }

    const file_path = try std.fmt.allocPrint(state.allocator, "{s}/{s}.bin", .{ DATA_DIR, upload_id });
    defer state.allocator.free(file_path);

    const stat = cwd.statFile(io, file_path, .{}) catch {
        try sendResponse(writer, 404, "text/plain", "File not found", connection);
        return;
    };

    var file = try cwd.openFile(io, file_path, .{});
    defer file.close(io);

    const disp = try std.fmt.allocPrint(state.allocator, "attachment; filename=\"{s}\"", .{filename});
    defer state.allocator.free(disp);

    const header = try std.fmt.allocPrint(state.allocator,
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Disposition: {s}\r\nContent-Length: {d}\r\nConnection: {s}\r\n\r\n",
        .{ disp, stat.size, connection });
    defer state.allocator.free(header);
    try writer.writeAll(header);

    var read_buf: [65536]u8 = undefined;
    var fr = file.reader(io, &read_buf);
    var r = &fr.interface;
    var buf: [65536]u8 = undefined;
    while (true) {
        const n = try r.readSliceShort(&buf);
        if (n == 0) break;
        try writer.writeAll(buf[0..n]);
    }
}

const ParsedRequest = struct {
    method: []const u8,
    path: []const u8,
    content_length: usize,
    upload_id: []const u8,
    chunk_idx: u32,
    total_chunks: u32,
    filename: []const u8,
    ttl: []const u8,
    total_size: u64,
    connection: []const u8,
};

fn parseRequestHeaders(data: []const u8) !ParsedRequest {
    var result = ParsedRequest{
        .method = "",
        .path = "",
        .content_length = 0,
        .upload_id = "",
        .chunk_idx = 0,
        .total_chunks = 0,
        .filename = "",
        .ttl = "",
        .total_size = 0,
        .connection = "keep-alive",
    };

    var lines = std.mem.splitScalar(u8, data, '\n');
    const first = lines.next() orelse return error.InvalidRequest;
    var parts = std.mem.splitScalar(u8, first, ' ');
    result.method = std.mem.trim(u8, (parts.next() orelse return error.InvalidRequest), "\r\n");
    result.path = std.mem.trim(u8, (parts.next() orelse return error.InvalidRequest), "\r\n");

    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, "\r\n");
        if (trimmed.len == 0) break;

        if (std.mem.startsWith(u8, trimmed, "Content-Length: ")) {
            result.content_length = std.fmt.parseInt(usize, trimmed[16..], 10) catch 0;
        } else if (std.mem.startsWith(u8, trimmed, "X-Upload-Id: ")) {
            result.upload_id = trimmed[13..];
        } else if (std.mem.startsWith(u8, trimmed, "X-Chunk-Index: ")) {
            result.chunk_idx = std.fmt.parseInt(u32, trimmed[15..], 10) catch 0;
        } else if (std.mem.startsWith(u8, trimmed, "X-Total-Chunks: ")) {
            result.total_chunks = std.fmt.parseInt(u32, trimmed[16..], 10) catch 0;
        } else if (std.mem.startsWith(u8, trimmed, "X-Filename: ")) {
            result.filename = trimmed[12..];
        } else if (std.mem.startsWith(u8, trimmed, "X-Ttl: ")) {
            result.ttl = trimmed[7..];
        } else if (std.mem.startsWith(u8, trimmed, "X-Total-Size: ")) {
            result.total_size = std.fmt.parseInt(u64, trimmed[14..], 10) catch 0;
        } else if (std.mem.startsWith(u8, trimmed, "Connection: ")) {
            result.connection = std.mem.trim(u8, trimmed[12..], "\r\n");
        }
    }

    return result;
}

// Upload ids are generated by the frontend as base62; only allow alphanumerics
// so they can never be used to escape the data/meta directories.
fn isValidId(id: []const u8) bool {
    if (id.len == 0 or id.len > 64) return false;
    for (id) |c| {
        if (!std.ascii.isAlphanumeric(c)) return false;
    }
    return true;
}

fn hexVal(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => null,
    };
}

fn urlDecode(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var out = try allocator.alloc(u8, input.len);
    var n: usize = 0;
    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '%' and i + 2 < input.len) {
            const hi = hexVal(input[i + 1]) orelse {
                out[n] = '%';
                n += 1;
                i += 1;
                continue;
            };
            const lo = hexVal(input[i + 2]) orelse {
                out[n] = '%';
                n += 1;
                i += 1;
                continue;
            };
            out[n] = hi * 16 + lo;
            n += 1;
            i += 3;
        } else {
            out[n] = input[i];
            n += 1;
            i += 1;
        }
    }
    return out[0..n];
}

// The web UI sends `encodeURIComponent(filename)`. Percent-decode it and strip
// characters that are unsafe in file names, JSON strings, or HTTP header
// values (which would allow CRLF header injection via the filename).
fn sanitizeFilename(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    const decoded = try urlDecode(allocator, raw);
    defer allocator.free(decoded);

    var out = try allocator.alloc(u8, decoded.len);
    var n: usize = 0;
    for (decoded) |c| {
        out[n] = if (c < 0x20 or c == 0x7f or c == '"' or c == '\\') '_' else c;
        n += 1;
    }
    return out[0..n];
}

fn handleChunkRequest(io: Io, addr: net.IpAddress, upload_id: []const u8, chunk_idx: u32, total_chunks: u32, filename: []const u8, ttl: []const u8, total_size: u64, body: []const u8, writer: *std.Io.Writer, connection: []const u8) !void {
    if (total_size > MAX_FILE_SIZE) {
        try sendResponse(writer, 413, "text/plain", "File too large (max 25GB)", connection);
        return;
    }
    if (total_chunks > MAX_CHUNKS) {
        try sendResponse(writer, 400, "text/plain", "Invalid total chunks", connection);
        return;
    }
    if (chunk_idx >= total_chunks) {
        try sendResponse(writer, 400, "text/plain", "Invalid chunk index", connection);
        return;
    }
    // Reject mismatched chunk sizes (also stops claiming a huge total_size
    // while sending tiny chunks).
    const offset: u64 = @as(u64, chunk_idx) * @as(u64, CHUNK_SIZE);
    const expected: usize = if (offset >= total_size)
        0
    else
        @intCast(@min(@as(u64, CHUNK_SIZE), total_size - offset));
    if (body.len != expected) {
        try sendResponse(writer, 400, "text/plain", "Chunk size mismatch", connection);
        return;
    }

    const ttl_seconds = parseTTL(ttl);
    const ip = try getClientIP(addr);
    defer state.allocator.free(ip);

    if (!try checkIPQuota(io, ip, total_size)) {
        try sendResponse(writer, 403, "text/plain", "Daily quota exceeded (25GB per IP)", connection);
        return;
    }

    // Create/refresh the upload entry and snapshot the metadata we need. We
    // must not keep pointers into the map across the slow chunk write below:
    // the cleanup thread may destroy the entry (and free its memory) if the
    // TTL expires mid-upload.
    var filename_copy: ?[]u8 = null;
    defer if (filename_copy) |f| state.allocator.free(f);
    const snapshot = snapshot: {
        state.mutex.lock();
        defer state.mutex.unlock();

        const gop = try state.uploads.getOrPut(upload_id);
        var meta: *UploadMeta = undefined;
        if (!gop.found_existing) {
            meta = try state.allocator.create(UploadMeta);
            meta.* = try UploadMeta.init(state.allocator, io, filename, total_size, total_chunks, ttl_seconds);
            // StringHashMap stores keys by reference; upload_id points into a
            // per-request stack buffer, so own a copy of it here.
            gop.key_ptr.* = try state.allocator.dupe(u8, upload_id);
            gop.value_ptr.* = meta;
        } else {
            meta = gop.value_ptr.*;
        }

        filename_copy = try state.allocator.dupe(u8, meta.filename);
        break :snapshot MetaSnapshot{
            .filename = filename_copy.?,
            .total_size = meta.total_size,
            .total_chunks = meta.total_chunks,
            .ttl_seconds = meta.ttl_seconds,
            .created_at = meta.created_at,
        };
    };

    // Write the chunk directly into its final position in <id>.bin. Chunks
    // arrive out of order (parallel uploads), but each one owns a disjoint
    // byte range, so positional writes are safe. createFile with
    // truncate=false opens the file if it exists and otherwise creates it,
    // without wiping out chunks written by other in-flight requests. The file
    // grows to exactly total_size once every chunk has been written.
    const bin_path = try std.fmt.allocPrint(state.allocator, "{s}/{s}.bin", .{ DATA_DIR, upload_id });
    defer state.allocator.free(bin_path);

    const cwd = std.Io.Dir.cwd();
    var out_file = try cwd.createFile(io, bin_path, .{ .read = true, .truncate = false });
    defer out_file.close(io);
    try out_file.writePositionalAll(io, body, offset);

    state.mutex.lock();
    const meta_ptr = state.uploads.getPtr(upload_id) orelse {
        state.mutex.unlock();
        try sendResponse(writer, 500, "text/plain", "Upload expired during upload", connection);
        return;
    };
    meta_ptr.*.chunks_received.set(chunk_idx);
    const is_complete = meta_ptr.*.isComplete();
    state.mutex.unlock();

    if (is_complete) {
        try saveMeta(io, upload_id, snapshot);
        try addIPQuota(ip, snapshot.total_size);
    }

    try sendResponse(writer, 200, "application/json", "{\"ok\":true}", connection);
}

fn handleConnection(io: Io, stream: net.Stream, addr: net.IpAddress) !void {
    defer stream.close(io);

    // Idle timeout for keep-alive: if no request bytes arrive on this
    // connection within the window, the next read fails and we close the
    // connection. During an active chunk body read the kernel resets the timer
    // on every recv, so slow uploads keep flowing as long as data arrives.
    const IdleTimeout = struct {
        tv_sec: i64,
        tv_usec: i64,
    };
    const tv = IdleTimeout{ .tv_sec = 60, .tv_usec = 0 };
    std.posix.setsockopt(stream.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};

    var read_buf: [8192]u8 = undefined;
    var reader_obj = stream.reader(io, &read_buf);
    var reader = &reader_obj.interface;

    var write_buf: [4096]u8 = undefined;
    var writer_obj = stream.writer(io, &write_buf);
    const writer = &writer_obj.interface;
    defer writer_obj.interface.flush() catch {};

    while (true) {
        // Read headers. Any error (client closed, idle timeout, stall) means
        // there is nothing more we can usefully serve on this connection.
        var header_buf: [8192]u8 = undefined;
        var header_len: usize = 0;
        var found_end = false;

        while (header_len < header_buf.len) {
            const byte = reader.takeByte() catch return;
            header_buf[header_len] = byte;
            header_len += 1;

            if (header_len >= 4 and std.mem.eql(u8, header_buf[header_len - 4 .. header_len], "\r\n\r\n")) {
                found_end = true;
                break;
            }
        }

        if (!found_end) {
            try sendResponse(writer, 400, "text/plain", "Bad request", "close");
            return;
        }

        const req = parseRequestHeaders(header_buf[0..header_len]) catch {
            try sendResponse(writer, 400, "text/plain", "Bad request", "close");
            return;
        };

        const keep_alive = !std.mem.eql(u8, req.connection, "close");
        const connection: []const u8 = if (keep_alive) "keep-alive" else "close";

        // Read body if present. Owned per-request, so free it explicitly before
        // looping (a defer here would pile up one allocation per request).
        var body: []u8 = &[_]u8{};
        var body_owned = false;
        var request_error = false;

        if (req.content_length > 0) {
            if (req.content_length > MAX_CHUNK_SIZE) {
                try sendResponse(writer, 413, "text/plain", "Chunk too large", connection);
                return;
            }

            body = try state.allocator.alloc(u8, req.content_length);
            body_owned = true;

            var received: usize = 0;
            while (received < req.content_length) {
                const n = reader.readSliceShort(body[received..]) catch {
                    request_error = true;
                    break;
                };
                if (n == 0) break;
                received += n;
            }
        }

        if (!request_error) {
            if (std.mem.eql(u8, req.path, "/")) {
                try sendFile(io, writer, "src/index.html", "text/html", connection);
            } else if (std.mem.eql(u8, req.path, "/a.png")) {
                try sendFile(io, writer, "src/a.png", "image/png", connection);
            } else if (std.mem.eql(u8, req.path, "/chunk")) {
                if (req.upload_id.len == 0 or req.total_chunks == 0 or !isValidId(req.upload_id)) {
                    try sendResponse(writer, 400, "text/plain", "Missing or invalid headers", connection);
                } else if (req.total_chunks > 1 and req.total_size == 0) {
                    try sendResponse(writer, 400, "text/plain", "Missing total size", connection);
                } else {
                    const safe_filename = try sanitizeFilename(state.allocator, req.filename);
                    try handleChunkRequest(io, addr, req.upload_id, req.chunk_idx, req.total_chunks, safe_filename, req.ttl, req.total_size, body, writer, connection);
                    state.allocator.free(safe_filename);
                }
            } else if (std.mem.startsWith(u8, req.path, "/s/")) {
                const id = req.path[3..];
                if (!isValidId(id)) {
                    try sendResponse(writer, 404, "text/plain", "Not found", connection);
                } else {
                    try sendDownload(io, writer, id, connection);
                }
            } else {
                try sendResponse(writer, 404, "text/plain", "Not found", connection);
            }
        }

        if (body_owned) state.allocator.free(body);
        try writer_obj.interface.flush();
        if (!keep_alive or request_error) return;
    }
}

const ExpiredUpload = struct {
    id: []u8,
};

fn cleanupThread(io: Io) !void {
    const cwd = std.Io.Dir.cwd();
    while (true) {
        io.sleep(Io.Duration.fromSeconds(60), .real) catch {};

        const now = nowSecs(io);
        var to_remove = std.ArrayList(ExpiredUpload).empty;
        defer {
            for (to_remove.items) |entry| state.allocator.free(entry.id);
            to_remove.deinit(state.allocator);
        }

        // Collect expired uploads under the lock. No `try` here: an allocation
        // failure must not exit the thread while it still holds the lock
        // (that would deadlock every other connection) or kill cleanup.
        state.mutex.lock();
        var it = state.uploads.iterator();
        while (it.next()) |entry| {
            const meta = entry.value_ptr.*;
            if (now > meta.created_at + @as(i64, @intCast(meta.ttl_seconds))) {
                const id = state.allocator.dupe(u8, entry.key_ptr.*) catch continue;
                to_remove.append(state.allocator, .{ .id = id }) catch {
                    state.allocator.free(id);
                };
            }
        }
        state.mutex.unlock();

        for (to_remove.items) |entry| {
            const file_path = std.fmt.allocPrint(state.allocator, "{s}/{s}.bin", .{ DATA_DIR, entry.id }) catch continue;
            defer state.allocator.free(file_path);
            cwd.deleteFile(io, file_path) catch {};

            const meta_path = std.fmt.allocPrint(state.allocator, "{s}/{s}.json", .{ META_DIR, entry.id }) catch continue;
            defer state.allocator.free(meta_path);
            cwd.deleteFile(io, meta_path) catch {};

            state.mutex.lock();
            if (state.uploads.fetchRemove(entry.id)) |kv| {
                state.allocator.free(kv.key);
                kv.value.deinit();
                state.allocator.destroy(kv.value);
            }
            state.mutex.unlock();
        }
    }
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = init.gpa;

    state = try ServerState.init(allocator, io);
    defer state.deinit();

    const args = try init.minimal.args.toSlice(allocator);
    defer allocator.free(args);

    const port: u16 = if (args.len > 1) std.fmt.parseInt(u16, args[1], 10) catch 8080 else 8080;

    const addr = try net.IpAddress.parseIp4("0.0.0.0", port);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    std.log.info("J3lyBin listening on http://0.0.0.0:{d}", .{port});

    // Start cleanup thread
    const cleanup_handle = try std.Thread.spawn(.{}, cleanupThread, .{io});
    cleanup_handle.detach();

    while (true) {
        const stream = try server.accept(io);
        const client_addr = stream.socket.address;

        const thread = std.Thread.spawn(.{}, handleConnection, .{ io, stream, client_addr }) catch |err| {
            std.log.err("Spawn error: {}", .{err});
            stream.close(io);
            continue;
        };
        thread.detach();
    }
}
