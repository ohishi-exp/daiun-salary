pub mod kudgivt;
pub mod kudguri;
pub mod work_segments;

use std::io::Read;

/// ZIP バイト列を展開し、(ファイル名, バイト列) のリストを返す
pub fn extract_zip(bytes: &[u8]) -> Result<Vec<(String, Vec<u8>)>, anyhow::Error> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;
    let mut files = Vec::new();
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let name = file.name().to_string();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        files.push((name, contents));
    }
    Ok(files)
}

/// Shift-JIS バイト列を UTF-8 文字列に変換
pub fn decode_shift_jis(bytes: &[u8]) -> String {
    let (decoded, _, _) = encoding_rs::SHIFT_JIS.decode(bytes);
    decoded.into_owned()
}

/// 運行NOでCSVデータをグループ化
/// 各CSVファイルから運行NOを抽出し、運行NO→行データのマップを返す
pub fn group_csv_by_unko_no(csv_text: &str) -> std::collections::HashMap<String, Vec<String>> {
    let mut map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    let mut lines = csv_text.lines();
    let _header = lines.next(); // skip header
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        // 運行NO is always the first column
        if let Some(unko_no) = line.split(',').next() {
            map.entry(unko_no.to_string())
                .or_default()
                .push(line.to_string());
        }
    }
    map
}

/// CSVテキストのヘッダー行を返す
pub fn csv_header(csv_text: &str) -> Option<&str> {
    csv_text.lines().next()
}
