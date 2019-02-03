CREATE TABLE "Task" (
  id            BIGSERIAL PRIMARY KEY,
  term          VARCHAR(50) NOT NULL,
  num           INT NOT NULL,
  priority      INT NOT NULL DEFAULT 0
);