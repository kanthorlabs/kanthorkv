# KanthorKV
> Key-Value database implementation based on database research at KanthorLabs

## Overview

KanthorKV is a research project at KanthorLabs to implement a key-value database inspired by the book "Database Design and Implementation" by Edward Sciore.

The quote that inspired this project is:

> “What I cannot create, I do not understand”
> – Richard Feynman, Physicist and Nobel Prize Winner

## Sub-projects

- A query parser that can parse a subset of SQL commands. Implement with python by using LALR(1) shift-reduce parser (Bottom-Up) same as PostgreSQL and SQLite does.

## Credits

- [Go implementation of SimpleDB from "Database Design and Implementation" - yokomotod](https://github.com/yokomotod/database-design-and-implementation-go)
- [Building thread-safe abstractions in Java versus Go](https://rybicki.io/blog/2024/11/03/multithreaded-code-java-golang.html)
- [simpledb-go](https://github.com/Chriscbr/simpledb-go)
- [simpledb](https://github.com/nakamasato/database-design-and-implementation/tree/main)

## Diary

- 2025-03-01: [Implement page and block_id](docs/diary/2025-03-01.md)
- 2025-03-09: [Implement file manager](docs/diary/2025-03-09.md)
- 2025-05-03: [Database memory management](docs/diary/2025-05-03.md)
- 2025-05-04: [Managing User Data](docs/diary/2025-05-04.md)
- 2025-05-25: [Implement a Buffer Manager](docs/diary/2025-05-25.md)
- 2025-06-01: [Implement Transaction Manager](docs/diary/2025-06-01.md)
- 2025-07-20: [Implement Transaction Manager](docs/diary/2025-07-20.md)
- 2025-08-03: [Implement Transaction](docs/diary/2025-08-03.md)
- 2025-08-23: [Record Manager](docs/diary/2025-08-23.md)
- 2025-08-24: [Record Manager](docs/diary/2025-08-24.md)
- 2025-09-07: [Record Manager](docs/diary/2025-09-07.md)
- 2025-09-20: [Scan](docs/diary/2025-09-20.md)
- 2025-09-27: [Parser](docs/diary/2025-09-27.md)
- 2025-09-28: [Planner](docs/diary/2025-09-28.md)
- 2025-09-29: [Index](docs/diary/2025-09-29.md)
