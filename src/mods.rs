use crate::osu_api::Mod;

pub fn build_mods_bitmap(mods: &[Mod]) -> u32 {
  let mut bitmap = 0;
  for m in mods {
    bitmap |= match m.acronym.as_str() {
      "NF" => 1 << 0,
      "EZ" => 1 << 1,
      "TD" => 1 << 2,
      "HD" => 1 << 3,
      "HR" => 1 << 4,
      "SD" => 1 << 5,
      "DT" => 1 << 6,
      "RX" => 1 << 7,
      "HT" | "DC" => 1 << 8,
      // NC also applies DT's bit
      "NC" => (1 << 9) | (1 << 6),
      "FL" => 1 << 10,
      "AT" => 1 << 11,
      "SO" => 1 << 12,
      "AP" => 1 << 13,
      // PF also applies SD's bit
      "PF" => (1 << 14) | (1 << 5),
      "4K" => 1 << 15,
      "5K" => 1 << 16,
      "6K" => 1 << 17,
      "7K" => 1 << 18,
      "8K" => 1 << 19,
      "FI" => 1 << 20,
      "RD" => 1 << 21,
      "CN" => 1 << 22,
      "TP" => 1 << 23,
      "9K" => 1 << 24,
      "CO" => 1 << 25,
      "1K" => 1 << 26,
      "3K" => 1 << 27,
      "2K" => 1 << 28,
      "V2" => 1 << 29,
      "MR" => 1 << 30,
      "CL" => 0,
      // Lazer-only and no impact on scores, so can be ignored
      "MU" | "AC" => 0,
      "TC" => {
        // Lazer-only; doesn't exist in the bitmap
        0
      },
      _ => {
        error!("Unknown mod: {}", m.acronym);
        0
      },
    };
  }
  bitmap
}
