Library of missing powershell commands for SQL server, used for making static reports
Copy to root reporting folder

use it: 
```powershell
Import-Module "C:\Folder\sql_Monitor.psm1" -Force
SQLDoc-PerfCounters-Collect -DataWarehouseServer Petar-Tr -DataWarehouseDatabase SQL_Datawarehouse 
```


Functions included

```powershell
Function Get-MissingIndexes     #v1.0
Function Get-unusedIndexes      #v1.0
Function Get-ExpensiveQueries   #v1.1 
Function Get-Warnings           #v1.1 
Function Match_Indexes          #v2.0 
Function Match_AG_Logins        #v2.0 
Function Get-BadPasswords       #v2.0 
Function Get-DefaultTrace       #v2.0 
```
