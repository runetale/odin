const std = @import("std");
const pg = @import("pg");

// USearchのC APIをインポート
const c = @cImport({
    @cInclude("usearch.h");
});

// 設定構造体
pub const Config = struct {
    host: []const u8,
    dbname: []const u8,
    user: []const u8,
    password: []const u8,
};

// グローバル変数: USearchインデックス
var usearchIndex: ?*c.usearch_index_t = null;

// 設定ファイルの読み込み
pub fn loadConfig(filePath: []const u8) !Config {
    const allocator = std.heap.page_allocator;
    const file = try std.fs.cwd().openFile(filePath, .{});
    defer file.close();

    const fileContents = try file.readAllAlloc(allocator, null);
    defer allocator.free(fileContents);

    const json = try std.json.parse(fileContents, .{});
    return Config{
        .host = try json.getString("database.host"),
        .dbname = try json.getString("database.dbname"),
        .user = try json.getString("database.user"),
        .password = try json.getString("database.password"),
    };
}

// データベース初期化
pub fn initDatabase(conn: *pg.Connection) !void {
    try conn.execute(
        \\CREATE TABLE IF NOT EXISTS time_series (
        \\    timestamp TIMESTAMPTZ NOT NULL,
        \\    sensor_id INT NOT NULL,
        \\    value REAL NOT NULL,
        \\    PRIMARY KEY (sensor_id, timestamp)
        \\);
    );
    try conn.execute("CREATE INDEX IF NOT EXISTS idx_time_series_timestamp ON time_series (timestamp);");
    std.debug.print("Database initialized successfully.\n", .{});
}

// データ挿入
pub fn insertData(conn: *pg.Connection, sensor_id: i32, timestamp: []const u8, value: f64) !void {
    try conn.query("INSERT INTO time_series (timestamp, sensor_id, value) VALUES ($1, $2, $3)", .{ timestamp, sensor_id, value });
    std.debug.print("Data inserted: sensor_id={d}, timestamp={s}, value={f}\n", .{ sensor_id, timestamp, value });
}

// データクエリ
pub fn queryData(conn: *pg.Connection, start: []const u8, end: []const u8) !void {
    const query = "SELECT * FROM time_series WHERE timestamp BETWEEN $1 AND $2";
    const result = try conn.query(query, .{ start, end });
    defer result.close();

    while (try result.nextRow()) {
        const timestamp = result.columnText(0);
        const sensor_id = result.columnInt(1);
        const value = result.columnFloat(2);
        std.debug.print("Timestamp: {s}, Sensor ID: {d}, Value: {f}\n", .{ timestamp, sensor_id, value });
    }
}

// 圧縮処理
pub fn compressData(conn: *pg.Connection) !void {
    try conn.execute(
        \\INSERT INTO compressed_time_series (day, avg_value, max_value, min_value)
        \\SELECT date_trunc('day', timestamp) AS day, AVG(value), MAX(value), MIN(value)
        \\FROM time_series
        \\WHERE timestamp < now() - interval '30 days'
        \\GROUP BY day;
    );
    std.debug.print("Data compressed successfully.\n", .{});
}

// デーモン化
pub fn daemonize() !void {
    const child = try std.process.spawn(.{
        .argv = &[_][]const u8{ "runetime-db", "compress" },
        .detached = true,
    });
    defer child.deinit();

    std.debug.print("Daemon started: PID={d}\n", .{child.getPid()});
}

// USearch初期化
pub fn initUSearch() !void {
    if (usearchIndex) |index| {
        c.usearch_free(index);
    }

    usearchIndex = c.usearch_create();
    if (usearchIndex == null) {
        @panic("Failed to initialize USearch");
    }
    std.debug.print("USearch index initialized.\n", .{});
}

// ベクトル追加
pub fn addVector(id: c.usearch_key_t, vector: []const f32) !void {
    if (usearchIndex == null) {
        try initUSearch();
    }

    if (c.usearch_add(usearchIndex.*, id, @ptrCast(*const f32, vector.ptr), vector.len) != 0) {
        @panic("Failed to add vector to USearch index");
    }
    std.debug.print("Vector added: ID={d}\n", .{id});
}

// ベクトル検索
pub fn searchVector(query_vector: []const f32, top_k: usize) !void {
    if (usearchIndex == null) {
        @panic("USearch index is not initialized");
    }

    var results_ids: [10]c.usearch_key_t = undefined;
    var results_distances: [10]f32 = undefined;

    const count = c.usearch_search(usearchIndex.*, @ptrCast(*const f32, query_vector.ptr), query_vector.len, results_ids.ptr, results_distances.ptr, top_k);

    if (count < 0) {
        @panic("Failed to search vector in USearch index");
    }

    for (0..count) |i| {
        std.debug.print("Result {d}: ID={d}, Distance={f}\n", .{ i, results_ids[i], results_distances[i] });
    }
}

// CLIメイン関数
pub fn main() !void {
    var args = try std.process.argsAlloc(std.heap.page_allocator);
    defer std.process.argsFree(std.heap.page_allocator, &args);

    if (args.len < 2) {
        std.debug.print("Usage: runetime-db <command> [options]\n", .{});
        return;
    }

    const config = try loadConfig("config.json");
    const conn = try pg.Connection.open(std.fmt.format("host={s} dbname={s} user={s} password={s}", .{ config.host, config.dbname, config.user, config.password }));
    defer conn.close();

    const command = args[1];
    if (std.mem.eql(u8, command, "init")) {
        try initDatabase(&conn);
    } else if (std.mem.eql(u8, command, "insert")) {
        if (args.len < 5) {
            std.debug.print("Usage: runetime-db insert <sensor_id> <timestamp> <value>\n", .{});
            return;
        }
        const sensor_id = try std.fmt.parseInt(i32, args[2], 10);
        const timestamp = args[3];
        const value = try std.fmt.parseFloat(f64, args[4]);
        try insertData(&conn, sensor_id, timestamp, value);
    } else if (std.mem.eql(u8, command, "query")) {
        if (args.len < 4) {
            std.debug.print("Usage: runetime-db query <start> <end>\n", .{});
            return;
        }
        const start = args[2];
        const end = args[3];
        try queryData(&conn, start, end);
    } else if (std.mem.eql(u8, command, "compress")) {
        try compressData(&conn);
    } else if (std.mem.eql(u8, command, "daemon")) {
        try daemonize();
    } else if (std.mem.eql(u8, command, "vector-add")) {
        if (args.len < 6) {
            std.debug.print("Usage: runetime-db vector-add <id> <x> <y> <z>\n", .{});
            return;
        }
        const id = try std.fmt.parseInt(c.usearch_key_t, args[2], 10);
        const vector: [3]f32 = [3]f32{
            try std.fmt.parseFloat(f32, args[3]),
            try std.fmt.parseFloat(f32, args[4]),
            try std.fmt.parseFloat(f32, args[5]),
        };
        try addVector(id, vector[0..]);
    } else if (std.mem.eql(u8, command, "vector-search")) {
        if (args.len < 6) {
            std.debug.print("Usage: runetime-db vector-search <x> <y> <z> <top_k>\n", .{});
            return;
        }
        const query_vector: [3]f32 = [3]f32{
            try std.fmt.parseFloat(f32, args[2]),
            try std.fmt.parseFloat(f32, args[3]),
            try std.fmt.parseFloat(f32, args[4]),
        };
        const top_k = try std.fmt.parseInt(usize, args[5], 10);
        try searchVector(query_vector[0..], top_k);
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
    }
}
