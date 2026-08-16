//! 出站文本处理:模型输出 → QQ 消息发送前的最后一道关卡。
//!
//! 职责(按顺序):
//! 1. `sanitize_reply` 清洗模型输出:统一换行、trim 首尾与行尾空白、
//!    折叠连续空行(3+ 换行 → 最多一个空行)、去除零宽字符 —— 解决
//!    「回复多几个换行符」「[记忆:..] 标记剥离后残留空行」;
//! 2. `cq_escape` 按 OneBot 11 CQ 码字符串规则转义 `&` `[` `]` ——
//!    模型输出中的 CQ 码字样不被 QQ 执行(注入防护);
//! 3. `segment_text` 按 max_msg_len 分段:优先在换行边界切,超长单行才硬切
//!    —— 解决「在词中间被截断」。
//!
//! 清洗发生在发送与落历史之前(历史存清洗后文本,与用户实际所见一致);
//! 转义只作用于真正发往 NapCat 的字符串(历史/轨迹保留原文)。

/// 清洗模型输出为适合 QQ 展示的文本。
pub fn sanitize_reply(text: &str) -> String {
    // 1) 统一换行为 \n,去除零宽字符
    let mut normalized = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\r' => {}
            '\u{200B}' | '\u{FEFF}' | '\u{200E}' | '\u{200F}' => {}
            _ => normalized.push(c),
        }
    }
    // 2) 行首尾空白去除(QQ 纯文本无缩进语义,顺带清掉模型输出的对齐残留)
    //    + 连续空行折叠为单个空行
    let mut out = String::with_capacity(normalized.len());
    let mut blank_run = 0usize;
    for line in normalized.split('\n') {
        let line = line.trim();
        if line.is_empty() {
            blank_run += 1;
            // 最多保留一个空行(即跳过第 2+ 个连续空行)
            if blank_run == 1 {
                out.push('\n');
            }
        } else {
            blank_run = 0;
            out.push_str(line);
            out.push('\n');
        }
    }
    // 3) 首尾 trim(去掉上面循环引入的首尾换行)
    out.trim().to_string()
}

/// OneBot 11 CQ 码字符串转义:发送纯文本时防注入。
/// 参考 https://github.com/botuniverse/onebot-11 的 CQ 码格式定义。
pub fn cq_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '[' => out.push_str("&#91;"),
            ']' => out.push_str("&#93;"),
            _ => out.push(c),
        }
    }
    out
}

/// 按 max_len 分段(字符数计):优先在换行边界切,单行超长才硬切。
/// 返回至少一段(输入非空时);空输入返回空切片。
pub fn segment_text(text: &str, max_len: usize) -> Vec<String> {
    let max_len = max_len.max(1);
    let mut chunks: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut cur_len = 0usize;
    for line in text.split('\n') {
        let line_len = line.chars().count();
        // 该行需要的宽度:当前已有内容时另起一行需要 +1(换行符)
        let needed = if cur.is_empty() { line_len } else { cur_len + 1 + line_len };
        if needed <= max_len {
            if !cur.is_empty() {
                cur.push('\n');
                cur_len += 1;
            }
            cur.push_str(line);
            cur_len += line_len;
            continue;
        }
        // 放不下一整行:先封存当前段,再处理本行
        if !cur.is_empty() {
            chunks.push(std::mem::take(&mut cur));
            cur_len = 0;
        }
        if line_len <= max_len {
            cur.push_str(line);
            cur_len = line_len;
        } else {
            // 单行超长:按字符硬切,最后一段留在 cur 里继续攒
            let mut rest = line;
            while rest.chars().count() > max_len {
                let head: String = rest.chars().take(max_len).collect();
                chunks.push(head);
                rest = &rest[rest.char_indices().nth(max_len).unwrap().0..];
            }
            cur.push_str(rest);
            cur_len = rest.chars().count();
        }
    }
    if !cur.is_empty() {
        chunks.push(cur);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_collapses_blank_runs() {
        assert_eq!(sanitize_reply("你好\n\n\n\n世界"), "你好\n\n世界");
        assert_eq!(sanitize_reply("  你好  \n  世界  \n"), "你好\n世界");
        // \r\n 与零宽字符
        assert_eq!(sanitize_reply("a\r\n\r\n\u{200B}b\u{FEFF}"), "a\n\nb");
        // 标记剥离后残留的空行被折叠
        assert_eq!(sanitize_reply("好的。\n\n[记忆:添加 x]\n\n还有吗"), "好的。\n\n[记忆:添加 x]\n\n还有吗");
        // 纯空白
        assert_eq!(sanitize_reply("\n\n  \n"), "");
    }

    #[test]
    fn cq_escape_basics() {
        assert_eq!(cq_escape("普通文本"), "普通文本");
        assert_eq!(cq_escape("[CQ:image,file=1.jpg]"), "&#91;CQ:image,file=1.jpg&#93;");
        assert_eq!(cq_escape("a&b"), "a&amp;b");
        assert_eq!(cq_escape("&#91;"), "&amp;#91;");
    }

    #[test]
    fn segment_prefers_newline_boundary() {
        // 短文本不分段
        assert_eq!(segment_text("你好", 10), vec!["你好"]);
        // 段落边界切分:不切断句子中间
        let text = "第一段话\n第二段话\n第三段话";
        let segs = segment_text(text, 9);
        assert!(segs.len() >= 2);
        assert_eq!(segs.join("\n"), text);
        for s in &segs {
            assert!(s.chars().count() <= 9, "段超长: {s}");
        }
        // 超长单行硬切
        let long = "一二三四五六七八九十".repeat(5);
        let segs = segment_text(&long, 20);
        assert!(segs.len() >= 3);
        assert_eq!(segs.concat(), long);
        // 空输入
        assert!(segment_text("", 10).is_empty());
    }
}
