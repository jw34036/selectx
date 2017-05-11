# selectx
JDBC extensions for dynamically processing relational queries

## Abstract
The purpose of this project is to provide a SQL-oriented solution for Java programs that need to query data in complex queries that involve advanced SQL constructs (outer joins, GROUP BY clauses, etc.) and result in data sets that do not map easily to an entity or domain model.  A full ORM solution provides no real benefit to these applications.  Furthermore, many large organizations have policy requirements for DBAs to review or even provide SQL statements to application developers, and their input can be challenging to implement in an object-centric design.

## Scope
- Will support SELECT statements only (the C,U, and D in CRUD benefit greatly from ORM stacks)
- No part of the JDBC API will need to be directly interacted with by the user
- Will work seamlessly with existing ORM and RDBMS infrastructure by staying as low-level as possible and doing as little as necessary
- Will not require embedding SQL in Java code; can read from external files.
