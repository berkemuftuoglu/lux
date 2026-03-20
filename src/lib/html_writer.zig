const std = @import("std");
const zul = @import("zul");

pub const HtmlWriter = struct {
    sb: zul.StringBuilder,

    pub fn init(allocator: std.mem.Allocator) HtmlWriter {
        return .{ .sb = zul.StringBuilder.init(allocator) };
    }

    pub fn deinit(self: *HtmlWriter) void {
        self.sb.deinit();
    }

    pub fn string(self: *const HtmlWriter) []const u8 {
        return self.sb.string();
    }

    /// Writes text with HTML escaping: & < > " '
    pub fn text(self: *HtmlWriter, s: []const u8) !void {
        for (s) |c| {
            switch (c) {
                '&' => try self.sb.write("&amp;"),
                '<' => try self.sb.write("&lt;"),
                '>' => try self.sb.write("&gt;"),
                '"' => try self.sb.write("&quot;"),
                '\'' => try self.sb.write("&#39;"),
                else => try self.sb.writeByte(c),
            }
        }
    }

    /// Writes raw HTML without escaping (trusted content only)
    pub fn raw(self: *HtmlWriter, s: []const u8) !void {
        try self.sb.write(s);
    }

    /// Writes an opening tag: <tagname>
    pub fn open(self: *HtmlWriter, tag: []const u8) !void {
        try self.sb.writeByte('<');
        try self.sb.write(tag);
        try self.sb.writeByte('>');
    }

    /// Writes a closing tag: </tagname>
    pub fn close(self: *HtmlWriter, tag: []const u8) !void {
        try self.sb.write("</");
        try self.sb.write(tag);
        try self.sb.writeByte('>');
    }

    /// Writes attribute: name="value" (value is NOT escaped — caller responsible)
    pub fn attr(self: *HtmlWriter, name: []const u8, value: []const u8) !void {
        try self.sb.write(name);
        try self.sb.write("=\"");
        try self.sb.write(value);
        try self.sb.writeByte('"');
    }

    /// Writes an integer as decimal string
    pub fn int(self: *HtmlWriter, n: anytype) !void {
        var w = self.sb.writer();
        try w.print("{d}", .{n});
    }
};

test "text: plain string passes through unchanged" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.text("hello");
    try std.testing.expectEqualStrings("hello", hw.string());
}

test "text: ampersand is escaped to &amp;" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.text("&");
    try std.testing.expectEqualStrings("&amp;", hw.string());
}

test "text: less-than is escaped to &lt;" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.text("<");
    try std.testing.expectEqualStrings("&lt;", hw.string());
}

test "text: greater-than is escaped to &gt;" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.text(">");
    try std.testing.expectEqualStrings("&gt;", hw.string());
}

test "text: double-quote is escaped to &quot;" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.text("\"");
    try std.testing.expectEqualStrings("&quot;", hw.string());
}

test "text: single-quote is escaped to &#39;" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.text("'");
    try std.testing.expectEqualStrings("&#39;", hw.string());
}

test "text: script tag XSS vector is fully escaped" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.text("<script>alert('xss')</script>");
    try std.testing.expectEqualStrings(
        "&lt;script&gt;alert(&#39;xss&#39;)&lt;/script&gt;",
        hw.string(),
    );
}

test "text: SQL injection with no HTML specials stays as-is" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.text("table\"; DROP TABLE users;--");
    try std.testing.expectEqualStrings("table&quot;; DROP TABLE users;--", hw.string());
}

test "raw: writes literal HTML without escaping" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.raw("<br>");
    try std.testing.expectEqualStrings("<br>", hw.string());
}

test "attr: writes name=value with quotes" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.attr("href", "javascript:void");
    try std.testing.expectEqualStrings("href=\"javascript:void\"", hw.string());
}

test "open: writes literal opening tag" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.open("div");
    try std.testing.expectEqualStrings("<div>", hw.string());
}

test "close: writes literal closing tag" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.close("div");
    try std.testing.expectEqualStrings("</div>", hw.string());
}

test "int: writes integer as decimal string" {
    var hw = HtmlWriter.init(std.testing.allocator);
    defer hw.deinit();
    try hw.int(42);
    try std.testing.expectEqualStrings("42", hw.string());
}
