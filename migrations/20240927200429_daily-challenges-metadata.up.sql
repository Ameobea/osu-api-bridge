CREATE TABLE daily_challenge_metadata (
  day_id INT PRIMARY KEY,
  room_id BIGINT NOT NULL,
  playlist_id BIGINT NOT NULL,
  current_playlist_item JSON NOT NULL
);
