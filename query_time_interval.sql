-- определяем время выполнения коммитов, включающие указанные файлы/дирректории, для первого года разработки
WITH 
-- 1. Выносим повторяющееся условие в отдельный CTE для улучшения читаемости и поддержки
target_commits AS (
    SELECT DISTINCT c.id, c.repo_id, c.commit_created_at
    FROM commits c
    JOIN file_changes fc ON c.id = fc.commit_id  -- Присоединяем информацию об изменяемых файлов в коммите
     -- Условие по пути к файлу и репозиторию
    WHERE (fc.file_path LIKE 'app/utils/excel_file_generator%' OR fc.file_path LIKE 'app/utils/simple_table_excel%') AND c.repo_id = 30
),
-- 2. Определяем год первого коммита среди отфильтрованных в CTE target_commits
first_commit_year AS (
    SELECT EXTRACT(YEAR FROM MIN(tc.commit_created_at)) AS start_year
    FROM target_commits tc -- Используем CTE с уже отфильтрованными коммитами
),
-- 3. Получаем коммиты за первый год разработки с информацией о родительском коммите
commits_with_first_parent AS (
    SELECT DISTINCT
        c.hash AS current_commit_hash,
        c.repo_id AS commit_repo_id,
        CASE  -- Используем первый элемент из parent_hashes, если он существует
            WHEN c.parent_hashes IS NULL OR array_length(c.parent_hashes, 1) = 0 THEN NULL
            ELSE c.parent_hashes[1]
        END AS first_parent_hash,
        c.commit_created_at AS current_commit_time,
        c.author_name,
        c.pull_request_id,
        c.repo_id
    FROM commits c
    JOIN target_commits tc ON c.id = tc.id AND c.repo_id = tc.repo_id -- Присоединяем через CTE
    CROSS JOIN first_commit_year fcy   -- Присоединяем найденный первый год
    WHERE EXTRACT(YEAR FROM c.commit_created_at) = fcy.start_year   -- Фильтруем по году, условие по файлам/репо уже учтено в tc
)
-- 4. Основной SELECT: вычисляем интервалы, присоединяем PR и Issue
SELECT
    cwp.repo_id,
    -- Вычисляем интервал как разницу между временем текущего и родительского коммита
    (cwp.current_commit_time - p.commit_created_at) AS time_interval,
    cwp.current_commit_time,
    p.commit_created_at AS parent_commit_time,
    cwp.author_name,
    cwp.current_commit_hash,
    cwp.first_parent_hash,
    cwp.pull_request_id AS pr_id,
    pr.title AS pr_title,
    i.issue_id,
    i.title AS issue_title,
    i.url AS issue_url
FROM commits_with_first_parent cwp
    -- Присоединяем таблицу commits снова, чтобы получить время родительского коммита
    JOIN commits p ON p.hash = cwp.first_parent_hash AND p.repo_id = cwp.commit_repo_id
    -- Присоединяем pull_requests, чтобы попытаться получить связь с задачами (issues)
    FULL JOIN pull_requests pr ON cwp.pull_request_id = pr.id
    -- Присоединяем issues, используя ANY для массива issue_ids
    FULL JOIN issues i ON i.id = ANY(pr.issue_ids) AND i.repo_id = pr.repo_id
WHERE cwp.first_parent_hash IS NOT NULL -- Исключаем коммиты без родителя (например, начальный коммит)
ORDER BY cwp.current_commit_time ASC; -- Сортировка по времени текущего коммита
