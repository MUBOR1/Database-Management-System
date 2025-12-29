-- ============================================
-- ЛАБОРАТОРНАЯ РАБОТА №1
-- PostgreSQL: основы и расширенные возможности
-- ============================================

-- Практикум 1
-- -----------

-- 1. Создание базы данных и подключение
-- $ createdb 7dbs
-- $ psql 7dbs

-- 2. Создание таблицы countries
CREATE TABLE countries (
    country_code char(2) PRIMARY KEY,
    country_name text UNIQUE
);

-- 3. Вставка данных в countries
INSERT INTO countries (country_code, country_name)
VALUES ('us', 'United States'), ('mx', 'Mexico'), ('au', 'Australia'),
       ('gb', 'United Kingdom'), ('de', 'Germany'), ('ll', 'Loompaland');

-- 4. Попытка вставить дубликат (должна вызвать ошибку)
-- INSERT INTO countries VALUES ('uk', 'United Kingdom');

-- 5. Проверка вставленных данных
SELECT * FROM countries;

-- 6. Удаление несуществующей страны
DELETE FROM countries WHERE country_code = 'll';

-- 7. Создание таблицы cities с внешним ключом
CREATE TABLE cities (
    name text NOT NULL,
    postal_code varchar(9) CHECK (postal_code <> ''),
    country_code char(2) REFERENCES countries,
    PRIMARY KEY (country_code, postal_code)
);

-- 8. Попытка вставить город с несуществующей страной (ошибка)
-- INSERT INTO cities VALUES ('Toronto', 'MACIBS', 'ca');

-- 9. Успешная вставка города
INSERT INTO cities VALUES ('Portland', '87200', 'us');

-- 10. Обновление почтового индекса
UPDATE cities SET postal_code = '97206' WHERE name = 'Portland';

-- 11. Создание таблицы venues с составным внешним ключом
CREATE TABLE venues (
    venue_id SERIAL PRIMARY KEY,
    name varchar(255),
    street_address text,
    type char(7) CHECK (type IN ('public', 'private')) DEFAULT 'public',
    postal_code varchar(9),
    country_code char(2),
    FOREIGN KEY (country_code, postal_code)
        REFERENCES cities (country_code, postal_code) MATCH FULL
);

-- 12. Вставка данных в venues
INSERT INTO venues (name, postal_code, country_code)
VALUES ('Crystal Ballroom', '97206', 'us');

-- 13. INNER JOIN городов и стран
SELECT cities.*, country_name
FROM cities INNER JOIN countries
ON cities.country_code = countries.country_code;

-- 14. Составное соединение venues и cities
SELECT v.venue_id, v.name, c.name
FROM venues v
INNER JOIN cities c
ON v.postal_code = c.postal_code AND v.country_code = c.country_code;

-- 15. Создание таблицы events
CREATE TABLE events (
    event_id SERIAL PRIMARY KEY,
    title text,
    starts timestamp,
    ends timestamp,
    venue_id integer REFERENCES venues(venue_id)
);

-- 16. Вставка событий
INSERT INTO events (title, starts, ends, venue_id) VALUES
('Fight Club', '2018-02-15 17:30:00', '2018-02-15 19:30:00', 1),
('April Fools Day', '2018-04-01 00:00:00', '2018-04-01 23:59:00', NULL),
('Christmas Day', '2018-12-25 00:00:00', '2018-12-25 23:59:00', NULL);

-- 17. LEFT JOIN событий и площадок
SELECT e.title, v.name
FROM events e LEFT JOIN venues v
ON e.venue_id = v.venue_id;

-- 18. Создание индексов
CREATE INDEX events_title_hash ON events USING hash (title);
CREATE INDEX events_starts_btree ON events USING btree (starts);

-- 19. Просмотр индексов
-- \di

-- 20. Задание 1.1: Выбор всех таблиц из pg_class
SELECT relname 
FROM pg_class 
WHERE relkind = 'r' 
  AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');

-- 21. Задание 1.2: Страна для "Fight Club"
SELECT c.country_name 
FROM events e
JOIN venues v ON e.venue_id = v.venue_id
JOIN cities ci ON v.country_code = ci.country_code AND v.postal_code = ci.postal_code
JOIN countries c ON ci.country_code = c.country_code
WHERE e.title = 'Fight Club';

-- 22. Задание 1.3: Добавление столбца active
ALTER TABLE venues ADD COLUMN active BOOLEAN DEFAULT TRUE;

-- Практикум 2
-- -----------

-- 23. Вставка дополнительных событий
INSERT INTO events (title, starts, ends, venue_id)
VALUES ('Moby', '2018-02-06 21:00', '2018-02-06 23:00', (
    SELECT venue_id FROM venues WHERE name = 'Crystal Ballroom'
));

INSERT INTO events (title, starts, ends, venue_id) VALUES
('Wedding', '2018-02-26 21:00:00', '2018-02-26 23:00:00', 1),
('Dinner with Mom', '2018-02-26 18:00:00', '2018-02-26 20:30:00', NULL),
('Valentine''s Day', '2018-02-14 00:00:00', '2018-02-14 23:59:00', NULL);

-- 24. Агрегатные функции
SELECT count(title) FROM events WHERE title LIKE '%Day%';

SELECT min(starts), max(ends)
FROM events INNER JOIN venues
    ON events.venue_id = venues.venue_id
WHERE venues.name = 'Crystal Ballroom';

-- 25. GROUP BY
SELECT venue_id, count(*) FROM events GROUP BY venue_id;

SELECT venue_id
FROM events
GROUP BY venue_id
HAVING count(*) >= 2 AND venue_id IS NOT NULL;

-- 26. Оконные функции
SELECT title, count(*) OVER (PARTITION BY venue_id) 
FROM events;

-- 27. Транзакции
BEGIN TRANSACTION;
DELETE FROM events;
ROLLBACK;
SELECT * FROM events;

-- 28. Хранимая процедура add_event
CREATE OR REPLACE FUNCTION add_event(
    title text,
    starts timestamp,
    ends timestamp,
    venue text,
    postal varchar(9),
    country char(2)
)
RETURNS boolean AS $$
DECLARE
    did_insert boolean := false;
    the_venue_id integer;
BEGIN
    SELECT venue_id INTO the_venue_id
    FROM venues v
    WHERE v.postal_code = postal 
      AND v.country_code = country 
      AND v.name ILIKE venue 
    LIMIT 1;

    IF the_venue_id IS NULL THEN
        INSERT INTO venues (name, postal_code, country_code)
        VALUES (venue, postal, country)
        RETURNING venue_id INTO the_venue_id;
        did_insert := true;
    END IF;

    RAISE NOTICE 'Venue found %', the_venue_id;

    INSERT INTO events (title, starts, ends, venue_id)
    VALUES (title, starts, ends, the_venue_id);

    RETURN did_insert;
END;
$$ LANGUAGE plpgsql;

-- 29. Вызов хранимой процедуры
SELECT add_event('House Party', '2018-05-03 23:00',
                 '2018-05-04 02:00', 'Run''s House', '97206', 'us');

-- 30. Триггер для логирования изменений событий
CREATE TABLE logs (
    event_id integer,
    old_title text,
    old_starts timestamp,
    old_ends timestamp,
    logged_at timestamp DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION log_event() RETURNS trigger AS $$
BEGIN
    INSERT INTO logs (event_id, old_title, old_starts, old_ends)
    VALUES (OLD.event_id, OLD.title, OLD.starts, OLD.ends);
    RAISE NOTICE 'Someone just changed event #%', OLD.event_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER log_events
AFTER UPDATE ON events
FOR EACH ROW EXECUTE PROCEDURE log_event();

-- 31. Тестирование триггера
UPDATE events
SET ends='2018-05-04 01:00:00'
WHERE title='House Party';

SELECT event_id, old_title, old_ends, logged_at FROM logs;

-- 32. Представление holidays
CREATE VIEW holidays AS
SELECT event_id AS holiday_id, title AS name, starts AS date
FROM events
WHERE title LIKE '%Day%' AND venue_id IS NULL;

-- 33. Запрос к представлению
SELECT name, to_char(date, 'Month DD, YYYY') AS date
FROM holidays
WHERE date <= '2018-04-01';

-- 34. Добавление массива цветов
ALTER TABLE events ADD colors text ARRAY;

CREATE OR REPLACE VIEW holidays AS
SELECT event_id AS holiday_id, title AS name, starts AS date, colors
FROM events
WHERE title LIKE '%Day%' AND venue_id IS NULL;

-- 35. Правило для обновления представления
CREATE RULE update_holidays AS ON UPDATE TO holidays DO INSTEAD
UPDATE events
SET title = NEW.name,
    starts = NEW.date,
    colors = NEW.colors
WHERE title = OLD.name;

-- 36. Задание 2.1: Правило для мягкого удаления
CREATE OR REPLACE RULE soft_delete_venue AS
ON DELETE TO venues DO INSTEAD
UPDATE venues SET active = FALSE WHERE venue_id = OLD.venue_id;

-- 37. Задание 2.2: Сводная таблица с generate_series
SELECT * FROM crosstab(
    'SELECT EXTRACT(YEAR FROM starts) AS year,
            EXTRACT(MONTH FROM starts) AS month,
            COUNT(*)
     FROM events
     GROUP BY year, month
     ORDER BY year, month',
    'SELECT * FROM generate_series(1,12)'
) AS (year INT, jan INT, feb INT, mar INT, apr INT, may INT, jun INT,
      jul INT, aug INT, sep INT, oct INT, nov INT, dec INT);

-- 38. Задание 2.3: Календарь событий по дням недели
SELECT *
FROM crosstab(
    'SELECT EXTRACT(WEEK FROM starts) AS week,
            EXTRACT(DOW FROM starts) AS day_of_week,
            COUNT(*)
     FROM events
     GROUP BY week, day_of_week
     ORDER BY week, day_of_week',
    'SELECT * FROM generate_series(0,6)'
) AS (week INT, sun INT, mon INT, tue INT, wed INT, thu INT, fri INT, sat INT);

-- Практикум 3
-- -----------

-- 39. Создание схемы для фильмов
CREATE TABLE genres (
    name text UNIQUE,
    position integer
);

CREATE TABLE movies (
    movie_id SERIAL PRIMARY KEY,
    title text,
    genre cube
);

CREATE TABLE actors (
    actor_id SERIAL PRIMARY KEY,
    name text
);

CREATE TABLE movies_actors (
    movie_id integer REFERENCES movies NOT NULL,
    actor_id integer REFERENCES actors NOT NULL,
    UNIQUE (movie_id, actor_id)
);

CREATE INDEX movies_actors_movie_id ON movies_actors (movie_id);
CREATE INDEX movies_actors_actor_id ON movies_actors (actor_id);
CREATE INDEX movies_genres_cube ON movies USING gist (genre);

-- 40. Нечеткий поиск
-- Установка расширений:
-- CREATE EXTENSION fuzzystrmatch;
-- CREATE EXTENSION pg_trgm;
-- CREATE EXTENSION cube;

-- LIKE и ILIKE
SELECT title FROM movies WHERE title ILIKE 'standust%';
SELECT title FROM movies WHERE title ILIKE 'standust_%';

-- Регулярные выражения
SELECT COUNT(*) FROM movies WHERE title !~* '^the.*';

-- Расстояние Левенштейна
SELECT levenshtein('bat', 'fads');
SELECT movie_id, title FROM movies
WHERE levenshtein(lower(title), lower('a hard day nght')) <= 3;

-- Триграммы
SELECT show_trgm('Avatar');

CREATE INDEX movies_title_trigram ON movies
USING gist (title gist_trgm_ops);

SELECT title FROM movies WHERE title % 'Avatre';

-- Полнотекстовый поиск
SELECT title FROM movies WHERE title @@ 'night & day';

SELECT to_tsvector('english', 'A Hard Day''s Night'),
       to_tsquery('english', 'night & day');

CREATE INDEX movies_title_searchable ON movies
USING gin(to_tsvector('english', title));

-- Метофоны
SELECT title
FROM movies NATURAL JOIN movies_actors NATURAL JOIN actors
WHERE metaphone(name, 6) = metaphone('Broos Wils', 6);

-- 41. Многомерный поиск с cube
-- Пример жанрового вектора для "Star Wars"
SELECT name,
       cube_ur_coord('(0,7,0,0,0,0,0,0,0,7,0,0,0,0,10,0,0,0)', position) as score
FROM genres g
WHERE cube_ur_coord('(0,7,0,0,0,0,0,0,0,7,0,0,0,0,10,0,0,0)', position) > 0;

-- Поиск похожих фильмов
SELECT title,
       cube_distance(genre, '(0,7,0,0,0,0,0,0,0,7,0,0,0,0,10,0,0,0)') dist
FROM movies
WHERE cube_enlarge('(0,7,0,0,0,0,0,0,0,7,0,0,0,0,10,0,0,0)'::cube, 5, 18)
      @> genre
ORDER BY dist;

-- 42. Задание 3.1: Хранимая процедура для рекомендаций
CREATE OR REPLACE FUNCTION get_movie_recommendations(search_term TEXT)
RETURNS TABLE(movie_title TEXT, similarity_score FLOAT) AS $$
BEGIN
    RETURN QUERY
    SELECT m.title, cube_distance(m.genre, target.genre) AS dist
    FROM movies m,
         (SELECT genre FROM movies WHERE title ILIKE '%' || search_term || '%' 
          UNION 
          SELECT genre FROM movies WHERE movie_id IN (
              SELECT movie_id FROM movies_actors WHERE actor_id IN (
                  SELECT actor_id FROM actors WHERE name ILIKE '%' || search_term || '%'
              )
          )
         ) AS target
    WHERE m.title NOT ILIKE '%' || search_term || '%'
    ORDER BY dist
    LIMIT 5;
END;
$$ LANGUAGE plpgsql;

-- 43. Задание 3.2: Расширение для комментариев
CREATE TABLE comments (
    comment_id SERIAL PRIMARY KEY,
    movie_id INT REFERENCES movies(movie_id),
    comment_text TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Извлечение ключевых слов
SELECT DISTINCT ts_lexize('english', unnest(string_to_array(comment_text, ' '))) AS keyword
FROM comments
WHERE ts_lexize('english', unnest(string_to_array(comment_text, ' '))) IS NOT NULL;

-- Самые обсуждаемые актёры
SELECT a.name, COUNT(*) AS mention_count
FROM actors a
JOIN movies_actors ma ON a.actor_id = ma.actor_id
JOIN movies m ON ma.movie_id = m.movie_id
JOIN comments c ON m.movie_id = c.movie_id
WHERE to_tsvector('english', c.comment_text) @@ to_tsquery('english', a.name)
GROUP BY a.name
ORDER BY mention_count DESC
LIMIT 10;

-- ============================================
-- КОНЕЦ ФАЙЛА
-- ============================================