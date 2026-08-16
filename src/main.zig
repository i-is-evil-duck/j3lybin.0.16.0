const std = @import("std");
const Io = std.Io;
const net = std.Io.net;

const MAX_FILE_SIZE: u64 = 5 * 1024 * 1024 * 1024; // 5GB
const MAX_DAILY_PER_IP: u64 = 10 * 1024 * 1024 * 1024; // 10GB
const CHUNK_SIZE: usize = 512 * 1024; // 512KB chunks
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
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.uploads.deinit();
        self.ip_quotas.deinit();
    }
};

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
var state: ServerState = undefined;

fn getClientIP(addr: net.IpAddress) ![]u8 {
    var buf: [64]u8 = undefined;
    const ip_str = try std.fmt.bufPrint(&buf, "{any}", .{addr});
    return try state.allocator.dupe(u8, ip_str);
}

fn checkIPQuota(io: Io, ip: []const u8, file_size: u64) !bool {
    state.ip_mutex.lock();
    defer state.ip_mutex.unlock();

    const now = nowSecs(io);
    const gop = try state.ip_quotas.getOrPut(ip);
    if (!gop.found_existing) {
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
    if (ttl_str.len < 2) return 48 * 3600;
    const num = std.fmt.parseInt(u64, ttl_str[0 .. ttl_str.len - 1], 10) catch return 48 * 3600;
    const unit = ttl_str[ttl_str.len - 1];
    return switch (unit) {
        'm' => num * 60,
        'h' => num * 3600,
        'w' => num * 7 * 86400,
        'M' => num * 30 * 86400,
        else => 48 * 3600,
    };
}

fn saveMeta(io: Io, upload_id: []const u8, meta: *const UploadMeta) !void {
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

fn assembleFile(io: Io, upload_id: []const u8, meta: *const UploadMeta) !void {
    const out_path = try std.fmt.allocPrint(state.allocator, "{s}/{s}.bin", .{ DATA_DIR, upload_id });
    defer state.allocator.free(out_path);

    const cwd = std.Io.Dir.cwd();
    var out_file = try cwd.createFile(io, out_path, .{});
    defer out_file.close(io);

    var write_buf: [CHUNK_SIZE]u8 = undefined;
    var w = out_file.writer(io, &write_buf);

    var i: u32 = 0;
    while (i < meta.total_chunks) : (i += 1) {
        const chunk_path = try std.fmt.allocPrint(state.allocator, "{s}/{s}.{d}", .{ DATA_DIR, upload_id, i });
        defer state.allocator.free(chunk_path);

        const chunk_data = try cwd.readFileAlloc(io, chunk_path, state.allocator, .limited(CHUNK_SIZE));
        defer state.allocator.free(chunk_data);

        try w.interface.writeAll(chunk_data);
        try cwd.deleteFile(io, chunk_path);
    }
    try w.flush();
}

fn sendResponse(writer: *std.Io.Writer, status: u16, content_type: []const u8, body: []const u8) !void {
    const status_text = switch (status) {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        413 => "Payload Too Large",
        else => "Unknown",
    };
    const header = try std.fmt.allocPrint(state.allocator,
        "HTTP/1.1 {d} {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ status, status_text, content_type, body.len });
    defer state.allocator.free(header);
    try writer.writeAll(header);
    try writer.writeAll(body);
}

fn sendFile(io: Io, writer: *std.Io.Writer, file_path: []const u8, content_type: []const u8) !void {
    const cwd = std.Io.Dir.cwd();
    const stat = cwd.statFile(io, file_path, .{}) catch {
        try sendResponse(writer, 404, "text/plain", "Not found");
        return;
    };

    var file = try cwd.openFile(io, file_path, .{});
    defer file.close(io);

    const header = try std.fmt.allocPrint(state.allocator,
        "HTTP/1.1 200 OK\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ content_type, stat.size });
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

fn sendDownload(io: Io, writer: *std.Io.Writer, upload_id: []const u8) !void {
    const cwd = std.Io.Dir.cwd();
    const meta_path = try std.fmt.allocPrint(state.allocator, "{s}/{s}.json", .{ META_DIR, upload_id });
    defer state.allocator.free(meta_path);

    const meta_data = cwd.readFileAlloc(io, meta_path, state.allocator, .limited(4096)) catch {
        try sendResponse(writer, 404, "text/plain", "Not found");
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

    const file_path = try std.fmt.allocPrint(state.allocator, "{s}/{s}.bin", .{ DATA_DIR, upload_id });
    defer state.allocator.free(file_path);

    const stat = cwd.statFile(io, file_path, .{}) catch {
        try sendResponse(writer, 404, "text/plain", "File not found");
        return;
    };

    var file = try cwd.openFile(io, file_path, .{});
    defer file.close(io);

    const disp = try std.fmt.allocPrint(state.allocator, "attachment; filename=\"{s}\"", .{filename});
    defer state.allocator.free(disp);

    const header = try std.fmt.allocPrint(state.allocator,
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Disposition: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ disp, stat.size });
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
        }
    }

    return result;
}

fn handleChunkRequest(io: Io, addr: net.IpAddress, upload_id: []const u8, chunk_idx: u32, total_chunks: u32, filename: []const u8, ttl: []const u8, total_size: u64, body: []const u8, writer: *std.Io.Writer) !void {
    if (total_size > MAX_FILE_SIZE) {
        try sendResponse(writer, 413, "text/plain", "File too large (max 5GB)");
        return;
    }

    const ttl_seconds = parseTTL(ttl);
    const ip = try getClientIP(addr);
    defer state.allocator.free(ip);

    if (!try checkIPQuota(io, ip, total_size)) {
        try sendResponse(writer, 403, "text/plain", "Daily quota exceeded (10GB per IP)");
        return;
    }

    state.mutex.lock();

    const gop = try state.uploads.getOrPut(upload_id);
    var meta: *UploadMeta = undefined;

    if (!gop.found_existing) {
        meta = try state.allocator.create(UploadMeta);
        meta.* = try UploadMeta.init(state.allocator, io, filename, total_size, total_chunks, ttl_seconds);
        gop.value_ptr.* = meta;
    } else {
        meta = gop.value_ptr.*;
    }

    if (chunk_idx >= meta.total_chunks) {
        state.mutex.unlock();
        try sendResponse(writer, 400, "text/plain", "Invalid chunk index");
        return;
    }

    state.mutex.unlock();

    // Save chunk
    const chunk_path = try std.fmt.allocPrint(state.allocator, "{s}/{s}.{d}", .{ DATA_DIR, upload_id, chunk_idx });
    defer state.allocator.free(chunk_path);

    const cwd = std.Io.Dir.cwd();
    var chunk_file = try cwd.createFile(io, chunk_path, .{});
    defer chunk_file.close(io);
    try chunk_file.writeStreamingAll(io, body);

    state.mutex.lock();
    meta.chunks_received.set(chunk_idx);
    const is_complete = meta.isComplete();
    state.mutex.unlock();

    if (is_complete) {
        try assembleFile(io, upload_id, meta);
        try addIPQuota(ip, total_size);
        try saveMeta(io, upload_id, meta);
    }

    try sendResponse(writer, 200, "application/json", "{\"ok\":true}");
}

fn handleConnection(io: Io, stream: net.Stream, addr: net.IpAddress) !void {
    defer stream.close(io);

    var read_buf: [8192]u8 = undefined;
    var reader_obj = stream.reader(io, &read_buf);
    var reader = &reader_obj.interface;

    var write_buf: [4096]u8 = undefined;
    var writer_obj = stream.writer(io, &write_buf);
    const writer = &writer_obj.interface;
    defer writer_obj.interface.flush() catch {};

    // Read headers
    var header_buf: [8192]u8 = undefined;
    var header_len: usize = 0;
    var found_end = false;

    while (header_len < header_buf.len) {
        const byte = reader.takeByte() catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        header_buf[header_len] = byte;
        header_len += 1;

        if (header_len >= 4 and std.mem.eql(u8, header_buf[header_len - 4 .. header_len], "\r\n\r\n")) {
            found_end = true;
            break;
        }
    }

    if (!found_end) {
        try sendResponse(writer, 400, "text/plain", "Bad request");
        return;
    }

    const req = parseRequestHeaders(header_buf[0..header_len]) catch {
        try sendResponse(writer, 400, "text/plain", "Bad request");
        return;
    };

    // Read body if present
    var body: []u8 = &[_]u8{};
    var body_owned = false;

    if (req.content_length > 0) {
        if (req.content_length > CHUNK_SIZE + 1024) {
            try sendResponse(writer, 413, "text/plain", "Chunk too large");
            return;
        }

        body = try state.allocator.alloc(u8, req.content_length);
        body_owned = true;
        errdefer { if (body_owned) state.allocator.free(body); }

        var received: usize = 0;
        while (received < req.content_length) {
            const n = try reader.readSliceShort(body[received..]);
            if (n == 0) break;
            received += n;
        }
    }
    defer { if (body_owned) state.allocator.free(body); }

    if (std.mem.eql(u8, req.path, "/")) {
        try sendFile(io, writer, "src/index.html", "text/html");
    } else if (std.mem.eql(u8, req.path, "/a.png")) {
        try sendFile(io, writer, "src/a.png", "image/png");
    } else if (std.mem.eql(u8, req.path, "/chunk")) {
        if (req.upload_id.len == 0 or req.total_chunks == 0) {
            try sendResponse(writer, 400, "text/plain", "Missing required headers");
            return;
        }
        try handleChunkRequest(io, addr, req.upload_id, req.chunk_idx, req.total_chunks, req.filename, req.ttl, req.total_size, body, writer);
    } else if (std.mem.startsWith(u8, req.path, "/s/")) {
        const id = req.path[3..];
        try sendDownload(io, writer, id);
    } else {
        try sendResponse(writer, 404, "text/plain", "Not found");
    }
}

fn cleanupThread(io: Io) !void {
    const cwd = std.Io.Dir.cwd();
    while (true) {
        io.sleep(Io.Duration.fromSeconds(60), .real) catch {};

        const now = nowSecs(io);
        var to_remove = std.ArrayList([]u8).empty;
        defer {
            for (to_remove.items) |id| state.allocator.free(id);
            to_remove.deinit(state.allocator);
        }

        state.mutex.lock();
        var it = state.uploads.iterator();
        while (it.next()) |entry| {
            const meta = entry.value_ptr.*;
            if (now > meta.created_at + @as(i64, @intCast(meta.ttl_seconds))) {
                const id = try state.allocator.dupe(u8, entry.key_ptr.*);
                try to_remove.append(state.allocator, id);
            }
        }
        state.mutex.unlock();

        for (to_remove.items) |id| {
            const file_path = std.fmt.allocPrint(state.allocator, "{s}/{s}.bin", .{ DATA_DIR, id }) catch continue;
            defer state.allocator.free(file_path);
            cwd.deleteFile(io, file_path) catch {};

            const meta_path = std.fmt.allocPrint(state.allocator, "{s}/{s}.json", .{ META_DIR, id }) catch continue;
            defer state.allocator.free(meta_path);
            cwd.deleteFile(io, meta_path) catch {};

            state.mutex.lock();
            if (state.uploads.fetchRemove(id)) |kv| {
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
