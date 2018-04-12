# db_operation_api


## 功能說明
將常用的資料庫操作 (PostgreSQL) 包裝起來，讓呼叫的程式不用處理 SQL 語法、資料庫細節，以及 psycopg2 的操作。
目前只支援少部分功能，以提供另一專案 [news_scraper](https://github.com/gn01842919/news_scraper) 所需功能為主。


## 使用範例
```python
from db_operation_api import PostgreSqlDB

db_config = {
    "db_host": 'localhost',
    "db_user": 'db_user',
    "db_password": 'password',
    "database": 'my_test_db',
}

with PostgreSqlDB(**db_config) as conn:

    # SELECT title, content, url FROM some_table WHERE id = 5;
    rows = conn.get_fields_by_conditions(
        "some_table",
        ("title", "content", "url"),
        {"id": 5}
    )
    title, content, url = rows[0]
    print(title, content, url)
```

## To-Do
- 目前我設定 PostgreSQL 為 autocommit = True, 也就是預設不使用 transaction。應增加 transaction 的功能 (也許用 with statement)。
