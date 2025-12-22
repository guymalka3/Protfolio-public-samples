# Appsflyer dbt project
Utilizing dbt for my existing appsflyer project to learn dbt.
## About dbt
**dbt** - data build tool. is a development framework that lets data analysts 
and engineers transform data in their warehouse using simple SQL. 
It handles the "T" (Transform) in ELT (Extract, Load, Transform).

Instead of writing complex, manual scripts to create tables, you write a SELECT statement.
dbt then handles the heavy lifting of turning that logic into a table or view in your database.
## Links
- [Project Docs](http://localhost:8080/#!/source_list/appsflyer_pl)
- Project Portfolio
***
## Access 
Using `subpltx_sa_dbt` for access in: 
- `guy-ppltx` as dev 
- `subpltx-dev` as prod 
***
# Main Commands
- The execution file - [run_dbt.sh](C:/workspace/ppltx-guy/dbt/appsflyer/run_dbt.sh)

Run bash file for the whole pipeline & tests - with alerts to slack
```bash
bash ~/workspace/ppltx-guy/dbt/appsflyer/run_dbt.sh
```
Or just run in this order (for windows):
```dtd
cd c:/workspace/ppltx-guy/dbt/appsflyer

.\dbt_venv\Scripts\Activate.ps1

dbt build
```
***
## Secondary Commands

Run for creating external table connection to bucket:
```dtd
dbt run-operation stage_external_sources --args "select: appsflyer_pl"
```
Run for the models' creation (will also connect to external storage):
```dtd
dbt run --select appsflyer
```
Run for the tests:
```dtd
dbt test --select appsflyer
```
***
