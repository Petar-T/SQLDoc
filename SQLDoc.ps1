function SQLDoc-Main-CollectGeneralInfo
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

    if ($ServerList)
        { SQLDoc-Common-CheckCollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -ServerList $ServerList 
        }
        else
        { SQLDoc-Common-CheckCollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase 
        }


$Query="SELECT  
  SERVERPROPERTY('MachineName') AS ComputerName,
  SERVERPROPERTY('ServerName') AS ServerName,  
  SERVERPROPERTY('InstanceName') AS InstanceName,
  (SELECT LOCAL_NET_ADDRESS AS 'IPAddressOfSQLServer' FROM SYS.DM_EXEC_CONNECTIONS WHERE SESSION_ID = @@SPID) as IP_address,
  SERVERPROPERTY('Edition') AS Edition,
  SERVERPROPERTY('ProductVersion') AS ProductVersion,  
  SERVERPROPERTY('ProductLevel') AS ProductLevel,  
  SERVERPROPERTY('ProductMajorVersion') AS ProductMajorVersion,
  SERVERPROPERTY('ProductMinorVersion') AS ProductMinorVersion,
  SERVERPROPERTY('ProductUpdateLevel') AS ProductUpdateLevel,
  SERVERPROPERTY('ProductUpdateReference') AS ProductUpdateReference,
  SERVERPROPERTY('IsHadrEnabled') AS IsHadrEnabled,
  SERVERPROPERTY('IsClustered') AS IsClustered,
  SERVERPROPERTY('IsBigDataCluster') AS IsBigDataCluster,
  SERVERPROPERTY('IsFullTextInstalled') AS IsFullTextInstalled,
  SERVERPROPERTY('IsAdvancedAnalyticsInstalled') AS IsAdvancedAnalyticsInstalled,
  SERVERPROPERTY('InstanceDefaultBackupPath') AS InstanceDefaultBackupPath,
  SERVERPROPERTY('InstanceDefaultDataPath') AS InstanceDefaultDataPath,
  SERVERPROPERTY('InstanceDefaultLogPath') AS InstanceDefaultLogPath,
  (SELECT SUBSTRING(physical_name, 1, CHARINDEX(N'master.mdf', LOWER(physical_name)) - 1) FROM master.sys.master_files WHERE database_id = 1 AND file_id = 1) as MasterDB_DataPath,
  (SELECT SUBSTRING(physical_name, 1, CHARINDEX(N'Tempdb.mdf', LOWER(physical_name)) - 1) FROM master.sys.master_files WHERE database_id = 2 AND file_id = 1) as TempDB_DataPath,
  SERVERPROPERTY('BuildClrVersion') AS BuildClrVersion,
  SERVERPROPERTY('Collation') AS Collation,
  --SERVERPROPERTY('CollationID') AS CollationID,
  SERVERPROPERTY('ComparisonStyle') AS ComparisonStyle,
  SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS ComputerNamePhysicalNetBIOS,
  SERVERPROPERTY('EngineEdition') AS EngineEdition,
  SERVERPROPERTY('FilestreamConfiguredLevel') AS FilestreamConfiguredLevel,
  SERVERPROPERTY('FilestreamEffectiveLevel') AS FilestreamEffectiveLevel,
  SERVERPROPERTY('FilestreamShareName') AS FilestreamShareName,
  SERVERPROPERTY('HadrManagerStatus') AS HadrManagerStatus,
  SERVERPROPERTY('IsIntegratedSecurityOnly') AS IsIntegratedSecurityOnly,
  SERVERPROPERTY('IsLocalDB') AS IsLocalDB,
  SERVERPROPERTY('IsPolyBaseInstalled') AS IsPolyBaseInstalled,
  SERVERPROPERTY('IsSingleUser') AS IsSingleUser,
  SERVERPROPERTY('IsTempDbMetadataMemoryOptimized') AS IsTempDbMetadataMemoryOptimized,
  SERVERPROPERTY('IsXTPSupported') AS IsXTPSupported,
  SERVERPROPERTY('LCID') AS LCID,
  SERVERPROPERTY('ProcessID') AS ProcessID,
  SERVERPROPERTY('ProductBuild') AS ProductBuild,
  SERVERPROPERTY('ProductBuildType') AS ProductBuildType,
  SERVERPROPERTY('ResourceLastUpdateDateTime') AS ResourceLastUpdateDateTime,
  SERVERPROPERTY('ResourceVersion') AS ResourceVersion,
  SERVERPROPERTY('SqlCharSet') AS SqlCharSet,
  SERVERPROPERTY('SqlCharSetName') AS SqlCharSetName,
  SERVERPROPERTY('SqlSortOrder') AS SqlSortOrder,
  SERVERPROPERTY('SqlSortOrderName') AS SqlSortOrderName,
  (SELECT cpu_count AS Logical_CPU_Count FROM sys.dm_os_sys_info) as Logical_CPU_Count , 
  (SELECT hyperthread_ratio AS Hyperthread_Ratio FROM sys.dm_os_sys_info) as Hyperthread_Ratio,
  (SELECT numa_node_count as numa_node_count FROM sys.dm_os_sys_info) as numa_node_count,
  (SELECT cpu_count/hyperthread_ratio AS Physical_CPU_Count FROM sys.dm_os_sys_info) as Physical_CPU_Count,
  (SELECT physical_memory_kb /1024 AS Physical_Memory_in_MB FROM sys.dm_os_sys_info) as Physical_Memory_in_MB,
  (SELECT virtual_machine_type_desc FROM sys.dm_os_sys_info) as virtual_machine_type_desc,
  (Select CEILING(physical_memory_kb/1024./1024) FROM sys.dm_os_sys_info) as physical_memory_GB , 
  (Select CEILING(CAST(value AS INT)/1024.) FROM sys.configurations WHERE name = 'max server memory (MB)') as MaxMemory,
  (Select sqlserver_start_time FROM sys.dm_os_sys_info) as LastReboot,
  (SELECT max_workers_count FROM sys.dm_os_sys_info) as MaxWorkers" 

  Write-Host "[$srv] initiating general collection" -ForegroundColor Yellow

    $scon = New-Object System.Data.SqlClient.SqlConnection
    $scon.ConnectionString = "Data Source=$DataWarehouseServer;Initial Catalog=$DataWarehouseDatabase;Integrated Security=true"
        
    $cmd = New-Object System.Data.SqlClient.SqlCommand
    $cmd.Connection = $scon
    $cmd.CommandTimeout = 0
    $scon.Open()
         
    $srvrs= Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "Select ServerName from [Target_Servers] where Is_Enabled=1"
    
    foreach($row in $srvrs)
    {
        $srv =$row.Item('ServerName')

        try
        {
            $mi= Invoke-Sqlcmd -Query $Query -ServerInstance $srv -ErrorAction Stop
            Write-Host "[$srv] connected.. " -ForegroundColor Green

        foreach ($row in $mi)
        {
            $P_01=    $row.Item('ComputerName') 
            $P_02=    $row.Item('ServerName') 
            $P_03=    $row.Item('InstanceName') 
            $P_04=    $row.Item('IP_address') 
            $P_05=    $row.Item('Edition') 
            $P_06=    $row.Item('ProductVersion') 
            $P_07=    $row.Item('ProductLevel') 
            $P_08=    $row.Item('ProductMajorVersion') 
            $P_09=    $row.Item('ProductMinorVersion') 
            $P_10=    $row.Item('ProductUpdateLevel') 
            $P_11=    $row.Item('ProductUpdateReference') 
            $P_12=    $row.Item('IsHadrEnabled') 
            $P_13=    $row.Item('IsClustered') 
            $P_14=    $row.Item('IsBigDataCluster') 
            $P_15=    $row.Item('IsFullTextInstalled') 
            $P_16=    $row.Item('IsAdvancedAnalyticsInstalled') 
            $P_17=    $row.Item('InstanceDefaultBackupPath') 
            $P_18=    $row.Item('InstanceDefaultDataPath') 
            $P_19=    $row.Item('InstanceDefaultLogPath') 
            $P_20=    $row.Item('MasterDB_DataPath') 
            $P_21=    $row.Item('TempDB_DataPath') 
            $P_22=    $row.Item('BuildClrVersion') 
            $P_23=    $row.Item('Collation') 
            $P_24=    $row.Item('ComparisonStyle') 
            $P_25=    $row.Item('ComputerNamePhysicalNetBIOS') 
            $P_26=    $row.Item('EngineEdition') 
            $P_27=    $row.Item('FilestreamConfiguredLevel') 
            $P_28=    $row.Item('FilestreamEffectiveLevel') 
            $P_29=    $row.Item('FilestreamShareName') 
            $P_30=    $row.Item('HadrManagerStatus') 
            $P_31=    $row.Item('IsIntegratedSecurityOnly') 
            $P_32=    $row.Item('IsLocalDB') 
            $P_33=    $row.Item('IsPolyBaseInstalled') 
            $P_34=    $row.Item('IsSingleUser') 
            $P_35=    $row.Item('IsTempDbMetadataMemoryOptimized') 
            $P_36=    $row.Item('IsXTPSupported') 
            $P_37=    $row.Item('LCID')
            $P_38=    $row.Item('ProcessID')
            $P_39=    $row.Item('ProductBuild') 
            $P_40=    $row.Item('ProductBuildType') 
            $P_41=    $row.Item('ResourceLastUpdateDateTime') 
            $P_42=    $row.Item('ResourceVersion') 
            $P_43=    $row.Item('SqlCharSet') 
            $P_44=    $row.Item('SqlCharSetName') 
            $P_45=    $row.Item('SqlSortOrder') 
            $P_46=    $row.Item('SqlSortOrderName') 
            $P_47=    $row.Item('Logical_CPU_Count')
            $P_48=    $row.Item('Hyperthread_Ratio')
            $P_49=    $row.Item('numa_node_count')
            $P_50=    $row.Item('Physical_CPU_Count')
            $P_51=    $row.Item('Physical_Memory_in_MB')
            $P_52=    $row.Item('virtual_machine_type_desc')
	        $P_53=    $row.Item('physical_memory_GB')
            $P_54=    $row.Item('MaxMemory')
            $P_55=    $row.Item('LastReboot')
            $P_56=    $row.Item('MaxWorkers')

            try
            {
                $cmd.CommandText = "EXEC dbo.GeneralInfo_Manage '$P_01','$P_02','$P_03','$P_04','$P_05','$P_06','$P_07','$P_08','$P_09','$P_10','$P_11','$P_12','$P_13','$P_14','$P_15','$P_16','$P_17','$P_18','$P_19','$P_20','$P_21','$P_22','$P_23','$P_24','$P_25','$P_26','$P_27','$P_28','$P_29','$P_30','$P_31','$P_32','$P_33','$P_34','$P_35','$P_36',$P_37,$P_38,'$P_39','$P_40','$P_41','$P_42','$P_43','$P_44','$P_45','$P_46',$P_47,$P_48,$P_49,$P_50,$P_51,'$P_52',$P_53,$P_54,'$P_55',$P_56"
                $cmd.ExecuteNonQuery() | Out-Null
                Write-Host "[$srv] collected! " -ForegroundColor Green
            }
            catch [Exception] 
            {
                Write-Warning $_.Exception.Message
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','General info [1]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null                  
            }
        }
                
        }
        Catch [Exception]
        {
                Write-Warning "Problem connecting $srv"
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','General info [2]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null
        }
        Write-Host ""
    } 

    $scon.Close()
    $scon.Dispose()
    $cmd.Dispose()

    SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'General Info collected!'

}

function SQLDoc-Common-CheckCollectionDB
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [System.IO.FileInfo]
        $ServerList
)

#region database
#check if Colection database exists 
    $sqlDB = "SELECT Count(*) as value_data FROM Sys.Databases where Name = N'$DataWarehouseDatabase'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database 'Master' -Query $sqlDB | select -expand value_data
    
    if ( $res -ne 1)
    {
      $sqlDBCreate = "Create database [$DataWarehouseDatabase]"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database 'Master' -Query $sqlDBCreate
      Write-host "Database $DataWarehouseDatabase created!" -ForegroundColor Yellow
    } 

#endregion database

#region tables
 #region tables : 'Missing_Indexes'

#check if Colection Table exists
    $obj_Name = 'Missing_Indexes'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[Missing_Indexes](
	[ServerName] [nvarchar](256) NOT NULL,
    [ServerEdition] [nvarchar](64) NULL,
    [EngineEdition] [int] NOT NULL,
	[Collection_Time] [datetime2](7) NULL,
	[First_Detected_date] [datetime2](7) NULL,
    [Last_Detected_date] [datetime2](7) NULL,
	[Number_of_Detections] [int] NULL,
	[Sqlserver_start_time] [datetime2](7) NULL,
	[DatabaseName] [nvarchar](256) NULL,
	[FullyQualifiedObjectName] [nvarchar](256) NULL,
	[ObjectID] [int] NULL,
	[SchemaName] [nvarchar](256) NULL,
	[TableName] [nvarchar](256) NOT NULL,
	[EqualityColumns] [nvarchar](max) NULL,
	[InEqualityColumns] [nvarchar](max) NULL,
	[IncludedColumns] [nvarchar](max) NULL,
	[Impact] [float] NULL,
	[UserSeeks] [bigint] NULL,
	[UserScans] [bigint] NULL,
	[UniqueCompiles] [bigint] NULL,
	[AvgTotalUserCost] [float] NULL,
	[AvgUserImpact] [float] NULL,
	[LastUserSeekTime] [datetime2](7) NULL,
	[LastUserScanTime] [datetime2](7) NULL,
	[SystemSeeks] [bigint] NULL,
	[SystemScans] [bigint] NULL,
	[LastSystemSeekTime] [datetime2](7) NULL,
	[LastSystemScanTime] [datetime2](7) NULL,
	[AvgTotalSystemCost] [float] NULL,
	[AvgSystemImpact] [float] NULL,
	[numberofIncludedFields] [int] NULL,
	[ProposedIndex_Hash] [nvarchar](32) NULL,
	[ProposedIndex] [nvarchar](max) NULL,
    [Total_Rows] [bigint] NULL, 
    [Total_Indexes] [int] NULL,
    [Total_Columns] [int] NULL
) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [IX_Hash] ON [dbo].[Missing_Indexes] ([ProposedIndex_Hash],[ServerName] )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    } 
#endregion tables : 'Missing_Indexes'

 #region tables : 'Collection_Errors'

#check if error Colection Table exists
    $obj_Name = 'Collection_Errors'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[Collection_Errors](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [nvarchar](128) NULL,
	[DateCollected] [datetime2](7) NOT NULL,
	[Collection_Type] [nvarchar](50) NOT NULL,
	[Error_Message] [varchar](max) NULL
) ON [PRIMARY] 
GO
ALTER TABLE [dbo].[Collection_Errors] ADD  CONSTRAINT [DF_Collection_Errors_DateCollected]  DEFAULT (getdate()) FOR [DateCollected]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    } 
 #endregion tables : 'Collection_Errors'

 #region tables : 'Target_Servers'
#check if Target_Servers Table exists
    $obj_Name = 'Target_Servers'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[Target_Servers](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [nvarchar](128) NULL,
	[Environment_Type] [nvarchar](50) NULL,
	[is_Enabled] Bit CONSTRAINT [df_Enabled] DEFAULT 1,
	[DateAdded] [datetime2](7) NOT NULL CONSTRAINT [df_DateAdded] DEFAULT GETDATE()
) ON [PRIMARY] 
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    }
 #endregion tables : 'Target_Servers'

 #region tables : 'Log_Table'
 #check if Log_Table Table exists
    $obj_Name = 'Log_Table'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[Log_Table](
	[Log_ID] [int] IDENTITY(1,1) NOT NULL,
	[DateAdded] [datetime2](7) NOT NULL CONSTRAINT [df_DateAdded1] DEFAULT GETDATE(),
	[UserName] [nvarchar](128) NULL,
	[Comment] [nvarchar](max) NULL 
) ON [PRIMARY] 
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    }
 #endregion tables : 'Log_Table'

 #region tables : 'SQL_GeneralInfo'

#check if GeneralInfo Table exists
    $obj_Name = 'SQL_GeneralInfo'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[SQL_GeneralInfo](
	[ComputerName] [nvarchar](50) NULL,
	[ServerName] [nvarchar](50) NULL,
	[InstanceName] [nvarchar](50) NULL,
	[IP_address] [nvarchar](20) NULL,
	[Edition] [nvarchar](50) NULL,
	[ProductVersion] [nvarchar](50) NULL,
	[ProductLevel] [nvarchar](50) NULL,
	[ProductMajorVersion] [nvarchar](50) NULL,
	[ProductMinorVersion] [nvarchar](50) NULL,
	[ProductUpdateLevel] [nvarchar](50) NULL,
	[ProductUpdateReference] [nvarchar](50) NULL,
	[IsHadrEnabled] [nvarchar](50) NULL,
	[IsClustered] [nvarchar](50) NULL,
	[IsBigDataCluster] [nvarchar](50) NULL,
	[IsFullTextInstalled] [nvarchar](50) NULL,
	[IsAdvancedAnalyticsInstalled] [nvarchar](50) NULL,
	[InstanceDefaultBackupPath] [nvarchar](255) NULL,
	[InstanceDefaultDataPath] [nvarchar](255) NULL,
	[InstanceDefaultLogPath] [nvarchar](255) NULL,
	[MasterDB_DataPath] [nvarchar](255) NULL,
	[TempDB_DataPath] [nvarchar](255) NULL,
	[BuildClrVersion] [nvarchar](50) NULL,
	[Collation] [nvarchar](50) NULL,
	[ComparisonStyle] [nvarchar](50) NULL,
	[ComputerNamePhysicalNetBIOS] [nvarchar](50) NULL,
	[EngineEdition] [nvarchar](50) NULL,
	[FilestreamConfiguredLevel] [nvarchar](50) NULL,
	[FilestreamEffectiveLevel] [nvarchar](50) NULL,
	[FilestreamShareName] [nvarchar](50) NULL,
	[HadrManagerStatus] [nvarchar](50) NULL,
	[IsIntegratedSecurityOnly] [nvarchar](50) NULL,
	[IsLocalDB] [nvarchar](50) NULL,
	[IsPolyBaseInstalled] [nvarchar](50) NULL,
	[IsSingleUser] [nvarchar](50) NULL,
	[IsTempDbMetadataMemoryOptimized] [nvarchar](50) NULL,
	[IsXTPSupported] [nvarchar](50) NULL,
	[LCID] [int] NULL,
	[ProcessID] [int] NULL,
	[ProductBuild] [nvarchar](50) NULL,
	[ProductBuildType] [nvarchar](50) NULL,
	[ResourceLastUpdateDateTime] [datetime] NULL,
	[ResourceVersion] [nvarchar](50) NULL,
	[SqlCharSet] [nvarchar](50) NULL,
	[SqlCharSetName] [nvarchar](50) NULL,
	[SqlSortOrder] [nvarchar](50) NULL,
	[SqlSortOrderName] [nvarchar](50) NULL,
	[Logical_CPU_Count] [int] NULL,
	[Hyperthread_Ratio] [int] NULL,
	[numa_node_count] [int] NULL,
	[Physical_CPU_Count] [int] NULL,
	[Physical_Memory_in_MB] [bigint] NULL,
	[virtual_machine_type_desc] [nvarchar](60) NULL,
	[physical_memory_GB] [numeric](30, 0) NULL,
	[MaxMemory] [numeric](16, 0) NULL,
	[LastReboot] [datetime] NULL,
	[MaxWorkers] [int] NULL,
	[Collect_Time] [datetime2](7) NULL
) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [IX_ServerName] ON [dbo].[SQL_GeneralInfo] ([ServerName] )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    } 

 #endregion tables : 'SQL_GeneralInfo'

 #region tables : 'JobInfo_Table'
 #check if JobInfo Table exists
    $obj_Name = 'JobInfo'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[JobInfo](
	[ServerName] [nvarchar](128) NULL,
	[agentsvc_status] [nvarchar](128) NULL,
	[agentsvc_startupType] [nvarchar](128) NULL,
	[JobName] [nvarchar](128) NULL,
	[Is_enabled] [tinyint] NULL,
	[Schedule_Name] [nvarchar](128) NULL,
	[Schedule_enabled] [int] NULL,
	[IsRunning] [int] NULL,
	[RequestSource] [nvarchar](128) NULL,
	[LastRunTime] [datetime] NULL,
	[NextRunTime] [datetime] NULL,
	[LastJobStep] [nvarchar](128) NULL,
	[RetryAttempt] [int] NULL,
	[JobLastOutcome] [varchar](20) NULL,
	[JobLastDuration] [int] NULL,
	[TotalRunsLast7Days] [int] NULL,
	[FailedRunsLast7Days] [int] NULL,
	[AverageDuration] [int] NULL,
	[MaxDuration] [int] NULL,
	[Collection_Time] [datetime] NULL
) ON [PRIMARY]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    }
 #endregion tables : 'JobInfo_Table'


 #region tables : 'PerfMon_Table'
 #check if PerfMon Table exists
    $obj_Name = 'PerfMon'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[PerfMon](
	[ServerName] [nvarchar](128) NULL,
    [Collection_Time] [datetime] NULL,
    [object_name] [nvarchar](128) NULL,
	[counter_name] [nvarchar](128) NULL,
	[counter_type] [int] NULL,
	[instance_name] [nvarchar](128) NULL,
	[counter_value] [bigint] NULL
) ON [PRIMARY]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    }
 #endregion tables : 'JobInfo_Table'



 #region tables : 'BackupInfo_Table'
 #check if BackupInfo Table exists
    $obj_Name = 'BackupInfo'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[BackupInfo](
	[ServerName] [nvarchar](128) NOT NULL,
	[DBname] [nvarchar](128) NOT NULL,
	[recovery_model] [nvarchar](20) NULL,
	[logSize_mb] [decimal](8, 2) NULL,
	[rowSize_mb] [decimal](8, 2) NULL,
	[totalSize_mb] [decimal](8, 2) NULL,
	[FullBck_count] [int] NULL,
	[LogBck_count] [int] NULL,
	[OtherBck_count] [int] NULL,
	[LastFullBackup] [datetime] NULL,
	[LastLogBackup] [datetime] NULL,
	[MaxFullDuration] [int] NULL,
	[MaxLogDuration] [int] NULL,
	[MaxBackupSize_mb] [bigint] NULL,
	[Collection_Date] [datetime] NULL
) ON [PRIMARY]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    }
 #endregion tables : 'BackupInfo_Table'

  #region tables : 'BadLoginInfo_Table'
 #check if BadLoginInfo Table exists
    $obj_Name = 'BadLoginInfo'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "create table [BadLoginInfo]
    ([ServerName] sysname,
    [LoginName] sysname,
    [IssueDescription] varchar(255),
    [LastUpdate] DateTime
) ON [PRIMARY]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    }
 #endregion tables : 'BadLoginInfo_Table'
 
 #region tables : 'AssessmentData_Table'
 #check if AssessmentData Table exists
    $obj_Name = 'AssessmentData'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE TABLE [dbo].[AssessmentData](
	[ServerName] [nvarchar](255) NOT NULL,
	[CheckName] [nvarchar](255) NULL,
	[CheckId] [nvarchar](255) NULL,
	[RulesetName] [nvarchar](50) NULL,
	[RulesetVersion] [nvarchar](50) NULL,
	[Severity] [nvarchar](50) NULL,
	[Message] [nvarchar](max) NULL,
	[Target_Server] [nvarchar](255) NULL,
	[Target_Database] [nvarchar](255) NOT NULL,
	[TargetType] [nvarchar](50) NULL,
	[HelpLink] [nvarchar](512) NULL,
	[Timestamp] [datetimeoffset](7) NULL
) ON [PRIMARY]
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Table $obj_Name created!" -ForegroundColor Yellow
    }
 #endregion tables : 'AssessmentData_Table'

#endregion Tables

#region Procedures and Views

 #region Stored procedure : 'Missing_Index_Manage'

#check if Missing_Index_Manage proc exists
    $obj_Name = 'Missing_Index_Manage'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE Procedure [dbo].[Missing_Index_Manage]
(
 @ServerName nvarchar(256),					--P01 , because I am lazy
 @ServerEdition nvarchar(50),				--P02
 @EngineEdition int,						--P03
 @Collection_Time datetime2(7),  			--P04
 @Sqlserver_start_time datetime2(7),		--P05
 @DatabaseName nvarchar(256),				--P06
 @FullyQualifiedObjectName nvarchar(256),	--P07
 @ObjectID int,								--P08
 @SchemaName nvarchar(256),					--P09
 @TableName nvarchar(256),					--P10
 @EqualityColumns nvarchar(max), 			--P11
 @InEqualityColumns nvarchar(max), 			--P12
 @IncludedColumns nvarchar(max), 			--P13
 @Impact float, 							--P14
 @UserSeeks bigint, 						--P15
 @UserScans bigint, 						--P16
 @UniqueCompiles bigint, 					--P17
 @AvgTotalUserCost float, 					--P18
 @AvgUserImpact float, 						--P19
 @LastUserSeekTime datetime2(7), 			--P20
 @LastUserScanTime datetime2(7), 			--P21
 @SystemSeeks bigint, 						--P22
 @SystemScans bigint, 						--P23
 @LastSystemSeekTime datetime2(7), 			--P24
 @LastSystemScanTime datetime2(7), 			--P25
 @AvgTotalSystemCost float, 				--P26
 @AvgSystemImpact float, 					--P27
 @numberofIncludedFields int, 				--P28
 @ProposedIndex_Hash nvarchar(32), 			--P29
 @ProposedIndex nvarchar(max)  				--P30
)					
AS
SET NOCOUNT ON;
IF EXISTS(select * from [dbo].[Missing_Indexes] where [ProposedIndex_Hash]=@ProposedIndex_Hash and [ServerName]=@ServerName)
	UPDATE [dbo].[Missing_Indexes]
	SET [Collection_Time] = @Collection_Time , 
       [ServerName] = @ServerName,
	   [Last_Detected_Date]=@Collection_Time,
		[Number_of_Detections]=[Number_of_Detections]+1,
       --[Sqlserver_start_time] = @Sqlserver_start_time,
	   [Sqlserver_start_time]=IIF([Sqlserver_start_time] <= @Sqlserver_start_time,[Sqlserver_start_time] , @Sqlserver_start_time),
       --[DatabaseName] = @DatabaseName,
       --[ObjectID] = @ObjectID,
       --[ObjectName] = @ObjectName,
       --[FullyQualifiedObjectName] = @FullyQualifiedObjectName,
       --[EqualityColumns] = @EqualityColumns,
       --[InEqualityColumns] = @InEqualityColumns,
       --[IncludedColumns] = @IncludedColumns,
       [Impact] = @Impact,
       --[UserSeeks] = @UserSeeks,
	   --[UserScans] = @UserScans,
	   [UserSeeks] = IIF([Sqlserver_start_time] <= @Sqlserver_start_time , @UserSeeks,@UserSeeks+[UserSeeks]),
	   [UserScans] = IIF([Sqlserver_start_time] <= @Sqlserver_start_time , @UserScans,@UserScans+[UserScans]),
       [UniqueCompiles] = @UniqueCompiles,
       [AvgTotalUserCost] = @AvgTotalUserCost,
       [AvgUserImpact] = @AvgUserImpact,
       [LastUserSeekTime] = @LastUserSeekTime,
       [LastUserScanTime] = @LastUserScanTime,
       [SystemSeeks] = @SystemSeeks,
       [SystemScans] = @SystemScans,
       [LastSystemSeekTime] = @LastSystemSeekTime,
       [LastSystemScanTime] = @LastSystemScanTime,
       [AvgTotalSystemCost] = @AvgTotalSystemCost,
       [AvgSystemImpact] = @AvgSystemImpact
       --[numberofIncludedFields] = @numberofIncludedFields,
       --[ProposedIndex] = @ProposedIndex 
 WHERE  (([ProposedIndex_Hash] = @ProposedIndex_Hash) and ([ServerName]=@ServerName))
ELSE
   insert into [dbo].[Missing_Indexes]
           ([ServerName]
		   ,[ServerEdition]
		   ,[EngineEdition]
           ,[Collection_Time]
		   ,[First_Detected_date]
		   ,[Last_Detected_date]
		   ,[Number_of_Detections]
           ,[Sqlserver_start_time]
           ,[DatabaseName]
           ,[FullyQualifiedObjectName]
           ,[ObjectID]
           ,[SchemaName]
           ,[TableName]
           ,[EqualityColumns]
           ,[InEqualityColumns]
           ,[IncludedColumns]
           ,[Impact]
           ,[UserSeeks]
           ,[UserScans]
           ,[UniqueCompiles]
           ,[AvgTotalUserCost]
           ,[AvgUserImpact]
           ,[LastUserSeekTime]
           ,[LastUserScanTime]
           ,[SystemSeeks]
           ,[SystemScans]
           ,[LastSystemSeekTime]
           ,[LastSystemScanTime]
           ,[AvgTotalSystemCost]
           ,[AvgSystemImpact]
           ,[numberofIncludedFields]
           ,[ProposedIndex_Hash]
           ,[ProposedIndex])
     VALUES
           (@ServerName,
		    @ServerEdition,
			@EngineEdition,
            @Collection_Time,
			@Collection_Time,
			@Collection_Time,
			1,
            @Sqlserver_start_time,
            @DatabaseName,
            @FullyQualifiedObjectName,
            @ObjectID,
			@SchemaName,
            @TableName,
            @EqualityColumns,
            @InEqualityColumns,
            @IncludedColumns,
            @Impact,
            @UserSeeks,
            @UserScans,
            @UniqueCompiles,
            @AvgTotalUserCost,
            @AvgUserImpact,
            @LastUserSeekTime,
            @LastUserScanTime,
            @SystemSeeks,
            @SystemScans,
            @LastSystemSeekTime,
            @LastSystemScanTime,
            @AvgTotalSystemCost,
            @AvgSystemImpact,
            @numberofIncludedFields, 
            @ProposedIndex_Hash, 
            @ProposedIndex)
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Procedure $obj_Name created!" -ForegroundColor Yellow
    } 

 #endregion Stored procedure : 'Missing_Index_Manage'

 #region Stored procedure : 'GeneralInfo_Manage'

#check if GeneralInfo_Manage proc exists
    $obj_Name = 'GeneralInfo_Manage'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE Procedure [dbo].[GeneralInfo_Manage]
(
 @ComputerName nvarchar(50),					--P01
 @ServerName nvarchar(50),						--P02
 @InstanceName nvarchar(50),					--P03
 @IP_address nvarchar(20),						--P04
 @Edition nvarchar(50),							--P05
 @ProductVersion nvarchar(50),					--P06
 @ProductLevel nvarchar(50),					--P07
 @ProductMajorVersion nvarchar(50),				--P08
 @ProductMinorVersion nvarchar(50),				--P09
 @ProductUpdateLevel nvarchar(50),				--P10
 @ProductUpdateReference nvarchar(50),			--P11
 @IsHadrEnabled nvarchar(50),					--P12
 @IsClustered nvarchar(50),						--P13
 @IsBigDataCluster nvarchar(50),				--P14
 @IsFullTextInstalled nvarchar(50),				--P15
 @IsAdvancedAnalyticsInstalled nvarchar(50),	--P16
 @InstanceDefaultBackupPath nvarchar(255),		--P17
 @InstanceDefaultDataPath nvarchar(255),		--P18
 @InstanceDefaultLogPath nvarchar(255),			--P19
 @MasterDB_DataPath nvarchar(255),				--P20
 @TempDB_DataPath nvarchar(255),				--P21
 @BuildClrVersion nvarchar(50),					--P22
 @Collation nvarchar(50),						--P23
 @ComparisonStyle nvarchar(50),					--P24
 @ComputerNamePhysicalNetBIOS nvarchar(50),		--P25
 @EngineEdition nvarchar(50),					--P26
 @FilestreamConfiguredLevel nvarchar(50),		--P27
 @FilestreamEffectiveLevel nvarchar(50),		--P28
 @FilestreamShareName nvarchar(50),				--P29
 @HadrManagerStatus nvarchar(50),				--P30
 @IsIntegratedSecurityOnly nvarchar(50),		--P31
 @IsLocalDB nvarchar(50),						--P32
 @IsPolyBaseInstalled nvarchar(50),				--P33
 @IsSingleUser nvarchar(50),					--P34
 @IsTempDbMetadataMemoryOptimized nvarchar(50),	--P35
 @IsXTPSupported nvarchar(50),					--P36
 @LCID int,										--P37
 @ProcessID int,								--P38
 @ProductBuild nvarchar(50),					--P39
 @ProductBuildType nvarchar(50),				--P40
 @ResourceLastUpdateDateTime  datetime,			--P41
 @ResourceVersion nvarchar(50),					--P42
 @SqlCharSet nvarchar(50),						--P43
 @SqlCharSetName nvarchar(50),					--P44
 @SqlSortOrder nvarchar(50),					--P45
 @SqlSortOrderName nvarchar(50),				--P46
 @Logical_CPU_Count int,						--P47
 @Hyperthread_Ratio int,						--P48
 @numa_node_count int,							--P49
 @Physical_CPU_Count int,						--P50
 @Physical_Memory_in_MB bigint,					--P51
 @virtual_machine_type_desc nvarchar(60),		--P52
 @physical_memory_GB numeric(30,0),				--P53
 @MaxMemory numeric(16,0),						--P54
 @LastReboot datetime,							--P55
 @MaxWorkers int								--P56
--@Collect_Time, datetime2(7),>)
) AS
SET NOCOUNT ON;
IF EXISTS(select * from [dbo].[SQL_GeneralInfo] where ServerName =@ServerName)
	UPDATE [dbo].[SQL_GeneralInfo]
	SET [ComputerName] = @ComputerName 
      ,[ServerName] = @ServerName
      ,[InstanceName] = @InstanceName
      ,[IP_address] = @IP_address
      ,[Edition] = @Edition
      ,[ProductVersion] = @ProductVersion
      ,[ProductLevel] = @ProductLevel
      ,[ProductMajorVersion] = @ProductMajorVersion
      ,[ProductMinorVersion] = @ProductMinorVersion
      ,[ProductUpdateLevel] = @ProductUpdateLevel
      ,[ProductUpdateReference] = @ProductUpdateReference
      ,[IsHadrEnabled] = @IsHadrEnabled
      ,[IsClustered] = @IsClustered
      ,[IsBigDataCluster] = @IsBigDataCluster
      ,[IsFullTextInstalled] = @IsFullTextInstalled
      ,[IsAdvancedAnalyticsInstalled] = @IsAdvancedAnalyticsInstalled
      ,[InstanceDefaultBackupPath] = @InstanceDefaultBackupPath
      ,[InstanceDefaultDataPath] = @InstanceDefaultDataPath
      ,[InstanceDefaultLogPath] = @InstanceDefaultLogPath
      ,[MasterDB_DataPath] = @MasterDB_DataPath
      ,[TempDB_DataPath] = @TempDB_DataPath
      ,[BuildClrVersion] = @BuildClrVersion
      ,[Collation] = @Collation
      ,[ComparisonStyle] = @ComparisonStyle
      ,[ComputerNamePhysicalNetBIOS] = @ComputerNamePhysicalNetBIOS
      ,[EngineEdition] = @EngineEdition
      ,[FilestreamConfiguredLevel] = @FilestreamConfiguredLevel
      ,[FilestreamEffectiveLevel] = @FilestreamEffectiveLevel
      ,[FilestreamShareName] = @FilestreamShareName
      ,[HadrManagerStatus] = @HadrManagerStatus
      ,[IsIntegratedSecurityOnly] = @IsIntegratedSecurityOnly
      ,[IsLocalDB] = @IsLocalDB
      ,[IsPolyBaseInstalled] = @IsPolyBaseInstalled
      ,[IsSingleUser] = @IsSingleUser
      ,[IsTempDbMetadataMemoryOptimized] = @IsTempDbMetadataMemoryOptimized
      ,[IsXTPSupported] = @IsXTPSupported
      ,[LCID] = @LCID
      ,[ProcessID] = @ProcessID
      ,[ProductBuild] = @ProductBuild
      ,[ProductBuildType] = @ProductBuildType
      ,[ResourceLastUpdateDateTime] = @ResourceLastUpdateDateTime
      ,[ResourceVersion] = @ResourceVersion
      ,[SqlCharSet] = @SqlCharSet
      ,[SqlCharSetName] = @SqlCharSetName
      ,[SqlSortOrder] = @SqlSortOrder
      ,[SqlSortOrderName] = @SqlSortOrderName
      ,[Logical_CPU_Count] = @Logical_CPU_Count
      ,[Hyperthread_Ratio] = @Hyperthread_Ratio
      ,[numa_node_count] = @numa_node_count
      ,[Physical_CPU_Count] = @Physical_CPU_Count
      ,[Physical_Memory_in_MB] = @Physical_Memory_in_MB
      ,[virtual_machine_type_desc] = @virtual_machine_type_desc
      ,[physical_memory_GB] = @physical_memory_GB
      ,[MaxMemory] = @MaxMemory
      ,[LastReboot] = @LastReboot
      ,[MaxWorkers] = @MaxWorkers
      ,[Collect_Time] = GetDate()
 WHERE [ServerName] = @ServerName
ELSE
INSERT INTO [dbo].[SQL_GeneralInfo]
      ([ComputerName]
      ,[ServerName]
      ,[InstanceName]
      ,[IP_address]
      ,[Edition]
      ,[ProductVersion]
      ,[ProductLevel]
      ,[ProductMajorVersion]
      ,[ProductMinorVersion]
      ,[ProductUpdateLevel]
      ,[ProductUpdateReference]
      ,[IsHadrEnabled]
      ,[IsClustered]
      ,[IsBigDataCluster]
      ,[IsFullTextInstalled]
      ,[IsAdvancedAnalyticsInstalled]
      ,[InstanceDefaultBackupPath]
      ,[InstanceDefaultDataPath]
      ,[InstanceDefaultLogPath]
      ,[MasterDB_DataPath]
      ,[TempDB_DataPath]
      ,[BuildClrVersion]
      ,[Collation]
      ,[ComparisonStyle]
      ,[ComputerNamePhysicalNetBIOS]
      ,[EngineEdition]
      ,[FilestreamConfiguredLevel]
      ,[FilestreamEffectiveLevel]
      ,[FilestreamShareName]
      ,[HadrManagerStatus]
      ,[IsIntegratedSecurityOnly]
      ,[IsLocalDB]
      ,[IsPolyBaseInstalled]
      ,[IsSingleUser]
      ,[IsTempDbMetadataMemoryOptimized]
      ,[IsXTPSupported]
      ,[LCID]
      ,[ProcessID]
      ,[ProductBuild]
      ,[ProductBuildType]
      ,[ResourceLastUpdateDateTime]
      ,[ResourceVersion]
      ,[SqlCharSet]
      ,[SqlCharSetName]
      ,[SqlSortOrder]
      ,[SqlSortOrderName]
      ,[Logical_CPU_Count]
      ,[Hyperthread_Ratio]
      ,[numa_node_count]
      ,[Physical_CPU_Count]
      ,[Physical_Memory_in_MB]
      ,[virtual_machine_type_desc]
      ,[physical_memory_GB]
      ,[MaxMemory]
      ,[LastReboot]
      ,[MaxWorkers]
      ,[Collect_Time])
     VALUES
      (@ComputerName, 
       @ServerName, 
       @InstanceName, 
       @IP_address, 
       @Edition, 
       @ProductVersion, 
       @ProductLevel, 
       @ProductMajorVersion, 
       @ProductMinorVersion, 
       @ProductUpdateLevel, 
       @ProductUpdateReference, 
       @IsHadrEnabled, 
       @IsClustered, 
       @IsBigDataCluster, 
       @IsFullTextInstalled, 
       @IsAdvancedAnalyticsInstalled, 
       @InstanceDefaultBackupPath, 
       @InstanceDefaultDataPath, 
       @InstanceDefaultLogPath, 
       @MasterDB_DataPath, 
       @TempDB_DataPath, 
       @BuildClrVersion, 
       @Collation, 
       @ComparisonStyle, 
       @ComputerNamePhysicalNetBIOS, 
       @EngineEdition, 
       @FilestreamConfiguredLevel, 
       @FilestreamEffectiveLevel, 
       @FilestreamShareName, 
       @HadrManagerStatus, 
       @IsIntegratedSecurityOnly, 
       @IsLocalDB, 
       @IsPolyBaseInstalled, 
       @IsSingleUser, 
       @IsTempDbMetadataMemoryOptimized, 
       @IsXTPSupported, 
       @LCID,
       @ProcessID,
       @ProductBuild, 
       @ProductBuildType, 
       @ResourceLastUpdateDateTime, 
       @ResourceVersion, 
       @SqlCharSet, 
       @SqlCharSetName, 
       @SqlSortOrder, 
       @SqlSortOrderName, 
       @Logical_CPU_Count,
       @Hyperthread_Ratio,
       @numa_node_count,
       @Physical_CPU_Count,
       @Physical_Memory_in_MB,
       @virtual_machine_type_desc,
	   @physical_memory_GB,
       @MaxMemory,
       @LastReboot,
       @MaxWorkers,
       GetDate())
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Procedure $obj_Name created!" -ForegroundColor Yellow
    } 

 #endregion Stored procedure : 'GeneralInfo_Manage'

 #region Stored procedure : 'JobInfo_Manage'

#check if JobInfo_Manage proc exists
    $obj_Name = 'JobInfo_Manage'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE Procedure [dbo].[JobInfo_Manage]
(
@ServerName nvarchar(128),				--P01
@agentsvc_status nvarchar(128),			--P02
@agentsvc_startupType nvarchar(128),	--P03
@JobName nvarchar(128),					--P04
@Is_enabled tinyint,					--P05
@Schedule_Name nvarchar(128),			--P06
@Schedule_enabled int,					--P07
@IsRunning int,							--P08
@RequestSource nvarchar(128),			--P09
@LastRunTime datetime,					--P10
@NextRunTime datetime,					--P11
@LastJobStep nvarchar(128),				--P12
@RetryAttempt int,						--P13
@JobLastOutcome nvarchar(20),			--P14
@JobLastDuration int,					--P15
@TotalRunsLast7Days int,				--P16
@FailedRunsLast7Days int,				--P17
@AverageDuration int,					--P18
@MaxDuration int 						--P19
)					
AS
SET NOCOUNT ON;
IF EXISTS(select * from [dbo].[JobInfo] where [JobName]=@JobName and [ServerName]=@ServerName)
	UPDATE [dbo].[JobInfo]
	SET [agentsvc_status] = @agentsvc_status,
    [agentsvc_startupType] = @agentsvc_startupType,
    [Is_enabled] = @Is_enabled, 
    [Schedule_Name] = @Schedule_Name,
    [Schedule_enabled] = @Schedule_enabled, 
    [IsRunning] = @IsRunning, 
    [RequestSource] = @RequestSource,
    [LastRunTime] = @LastRunTime, 
    [NextRunTime] = @NextRunTime,
    [LastJobStep] = @LastJobStep,
    [RetryAttempt] = @RetryAttempt,
    [JobLastOutcome] = @JobLastOutcome,
    [JobLastDuration] = @JobLastDuration,
    [TotalRunsLast7Days] = @TotalRunsLast7Days, 
    [FailedRunsLast7Days] = @FailedRunsLast7Days,
    [AverageDuration] = @AverageDuration,  
    [MaxDuration] = @MaxDuration,  
    [Collection_Time] = GetDate() 
 WHERE ([JobName]=@JobName and [ServerName]=@ServerName)
 ELSE
 INSERT INTO [dbo].[JobInfo]
           ([ServerName]
           ,[agentsvc_status]
           ,[agentsvc_startupType]
           ,[JobName]
           ,[Is_enabled]
           ,[Schedule_Name]
           ,[Schedule_enabled]
           ,[IsRunning]
           ,[RequestSource]
           ,[LastRunTime]
           ,[NextRunTime]
           ,[LastJobStep]
           ,[RetryAttempt]
           ,[JobLastOutcome]
           ,[JobLastDuration]
           ,[TotalRunsLast7Days]
           ,[FailedRunsLast7Days]
           ,[AverageDuration]
           ,[MaxDuration]
           ,[Collection_Time])
     VALUES
           (@ServerName,
           @agentsvc_status, 
           @agentsvc_startupType, 
           @JobName, 
           @Is_enabled, 
           @Schedule_Name, 
           @Schedule_enabled,
           @IsRunning,
           @RequestSource, 
           @LastRunTime, 
           @NextRunTime, 
           @LastJobStep, 
           @RetryAttempt,
           @JobLastOutcome, 
           @JobLastDuration,
           @TotalRunsLast7Days,
           @FailedRunsLast7Days,
           @AverageDuration,
           @MaxDuration,
           GetDate() )
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Procedure $obj_Name created!" -ForegroundColor Yellow
    } 

 #endregion Stored procedure : 'JobInfo_Manage'



  #region Stored procedure : 'PerfMon_Manage'

#check if JobInfo_Manage proc exists
    $obj_Name = 'PerfMon_Manage'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE Procedure [dbo].[PerfMon_Manage]
(
@ServerName nvarchar(128),				--P01
@Collection_Time datetime,		    	--P02
@object_name nvarchar(128),	            --P03
@counter_name nvarchar(128),			--P04
@cntr_type int,					        --P05
@instance_name nvarchar(128),			--P06
@Cntr_value bigint					    --P07
)					
AS
SET NOCOUNT ON;
 INSERT INTO [dbo].[PerfMon]
           ([ServerName]
           ,[Collection_Time]
           ,[object_name]
           ,[counter_name]
           ,[counter_type]
           ,[instance_name]
           ,[counter_value])
     VALUES
           (@ServerName,
           @Collection_Time, 
           @object_name, 
           @counter_name, 
           @cntr_type, 
           @instance_name, 
           @Cntr_value)
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Procedure $obj_Name created!" -ForegroundColor Yellow
    } 

 #endregion Stored procedure : 'PerfMon_Manage'

 #region Stored procedure : 'BackupInfo_Manage'
#check if BackupInfo_Manage proc exists
    $obj_Name = 'BackupInfo_Manage'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE Procedure [dbo].[BackupInfo_Manage]
(
@ServerName nvarchar(128),		--P01
@DBname nvarchar(128),			--P02
@recovery_model  nvarchar(20),	--P03
@logSize_mb decimal(8,2),		--P04
@rowSize_mb decimal(8,2),		--P05
@totalSize_mb decimal(8,2),		--P06
@FullBck_count int,				--P07
@LogBck_count int,				--P08
@OtherBck_count int,			--P09
@LastFullBackup datetime,		--P10
@LastLogBackup datetime,		--P11
@MaxFullDuration int,			--P12
@MaxLogDuration int,			--P13
@MaxBackupSize_mb bigint,		--P14
@Collection_Date datetime 		--P15
)					
AS
SET NOCOUNT ON;
IF EXISTS(select * from [dbo].[BackupInfo] where [DBName]=@DBName and [ServerName]=@ServerName)
	UPDATE [dbo].[BackupInfo]
SET [recovery_model]=@recovery_model,
	[logSize_mb]=@logSize_mb,
	[rowSize_mb]=@rowSize_mb,
	[totalSize_mb]=@totalSize_mb,
	[FullBck_count]=@FullBck_count,
	[LogBck_count]=@LogBck_count,
	[OtherBck_count]=@OtherBck_count,
	[LastFullBackup]=@LastFullBackup,
	[LastLogBackup]=@LastLogBackup,
	[MaxFullDuration]=@MaxFullDuration,
	[MaxLogDuration]=@MaxLogDuration,
	[MaxBackupSize_mb]=@MaxBackupSize_mb,
	[Collection_Date]=@Collection_Date
 WHERE ([DBName]=@DBName and [ServerName]=@ServerName)
 ELSE
 INSERT INTO [dbo].[BackupInfo]
           ([ServerName]
           ,[DBname]
           ,[recovery_model]
           ,[logSize_mb]
           ,[rowSize_mb]
           ,[totalSize_mb]
           ,[FullBck_count]
           ,[LogBck_count]
           ,[OtherBck_count]
           ,[LastFullBackup]
           ,[LastLogBackup]
           ,[MaxFullDuration]
           ,[MaxLogDuration]
           ,[MaxBackupSize_mb]
           ,[Collection_Date])
     VALUES
           (@ServerName,
           @DBname,
           @recovery_model,
           @logSize_mb,
           @rowSize_mb,
           @totalSize_mb,
           @FullBck_count,
           @LogBck_count,
           @OtherBck_count,
           @LastFullBackup,
           @LastLogBackup,
           @MaxFullDuration,
           @MaxLogDuration,
           @MaxBackupSize_mb,
           @Collection_Date)
    GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Procedure $obj_Name created!" -ForegroundColor Yellow
    } 
 #endregion Stored procedure : 'BackupInfo_Manage'

 #region Stored procedure : 'BadLogin_Manage'
 #check if BadLoginInfo_Manage proc exists
    $obj_Name = 'BadLoginInfo_Manage'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE Procedure [dbo].[BadLoginInfo_Manage]
(
@ServerName sysname,	--P01
@LoginName sysname,		--P02
@Issue nvarchar(128)	--P03
)					
AS
SET NOCOUNT ON;

IF EXISTS(select * from [dbo].[BadLoginInfo] where [ServerName]=@ServerName and [LoginName]=@LoginName and [IssueDescription]= @Issue  )
	UPDATE [dbo].[BadLoginInfo]
	SET [LastUpdate]=GetDate()
	WHERE ([ServerName]=@ServerName and [LoginName]=@LoginName and [IssueDescription]= @Issue )
 ELSE
 INSERT INTO [dbo].[BadLoginInfo]
           ([ServerName],[LoginName],[IssueDescription],[LastUpdate])
		   VALUES  (@ServerName,@LoginName,@Issue,GetDate())
    GO"

      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Procedure $obj_Name created!" -ForegroundColor Yellow
    } 

 #endregion Stored procedure : 'BadLogin_Manage'

 #region Stored procedure : 'AssessmentData_Manage'

#check if AssessmentData proc exists
    $obj_Name = 'AssessmentData_Manage'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE Procedure [dbo].[AssessmentData_Manage]
(
 @ServerName nvarchar(255),			--P01 , because I am lazy
 @CheckName nvarchar(255),			--P02
 @CheckId nvarchar(255),			--P03
 @RulesetName nvarchar(50),  		--P04
 @RulesetVersion nvarchar(50),		--P05
 @Severity nvarchar(50),			--P06
 @Message nvarchar(max),			--P07
 @Target_Path nvarchar(255),		--P08
 @TargetType nvarchar(50),			--P09
 @HelpLink nvarchar(max) 			--P10
  )					
AS
SET NOCOUNT ON;
INSERT INTO [dbo].[AssessmentData]
           ([ServerName]
           ,[CheckName]
           ,[CheckId]
           ,[RulesetName]
           ,[RulesetVersion]
           ,[Severity]
           ,[Message]
           ,[Target_Server]
           ,[Target_Database]
           ,[TargetType]
           ,[HelpLink]
           ,[Timestamp])
     VALUES
           (@ServerName,
           @CheckName, 
           @CheckId, 
           @RulesetName, 
           @RulesetVersion, 
           @Severity, 
           @Message, 
           [dbo].[GetAssessment_ObjectName] ( 'Server',@Target_Path ) , 
           [dbo].[GetAssessment_ObjectName] ( 'Database',@Target_Path ), 
           @TargetType, 
           @HelpLink, 
           GetDate()) 
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Procedure $obj_Name created!" -ForegroundColor Yellow
    } 
 #endregion Stored procedure : 'AssessmentData_Manage'

 #region UDF : 'GetAssessment_ObjectName'

#check if GetAssessment_ObjectName func exists
    $obj_Name = 'GetAssessment_ObjectName'
    $sqlObj = "SELECT Count(*) as value_data FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = '$obj_Name'"
    $res = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlObj | select -expand value_data

    if ( $res -ne 1)
    {
    $sqlCreateObj = "CREATE FUNCTION [dbo].[GetAssessment_ObjectName] 
(@type varchar(20)  , @instr varchar(255) )
RETURNS varchar(128) AS
BEGIN
Declare @p int
Declare @ret varchar(128)
SELECT @p = charindex(@Type, @instr); 

If @p> 0 
	BEGIN
	Set @ret=SUBSTRING(@Instr,@p+Len(@Type)+8,128) 
	Set @ret=SUBSTRING(@ret,1,charindex(CHAR(42),@ret)-1) 
	END
	else
Set @ret=''
    RETURN @ret
END
GO"
      Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlCreateObj
      Write-host "Procedure $obj_Name created!" -ForegroundColor Yellow
    } 
 #endregion UDF : 'GetAssessment_ObjectName'
 
#endregion Procedures and Views


    if ($ServerList)
    { 
    Try{
        $srvrs=get-content -Path $ServerList -ErrorAction Stop

        foreach($srv in $srvrs)
            {
                $SQL_Add="IF NOT EXISTS(select * from [dbo].[Target_Servers] where ServerName ='$srv')
                          insert into [dbo].[Target_Servers] ([ServerName]) VALUES ('$srv')"

            $mi= Invoke-Sqlcmd -Query  $SQL_Add -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -ErrorAction Stop
            }
            Write-host "Server list initialized from file!" -ForegroundColor Yellow
       }
       catch
       { 
           Write-Host "Problem initializing servers list from file" 
       }
    }

}

Function SQLDoc-Common-MakeLogEntry
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $true)]
        [String]
        $Message
  
)
try
            {
                $UsrIns=[System.Security.Principal.WindowsIdentity]::GetCurrent().Name
                $cmdIns= "INSERT INTO [dbo].[Log_Table] ([UserName],[Comment])
                    VALUES ('$UsrIns','$Message')"
                 Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $cmdIns
            }
            catch [Exception] 
            {
                Write-Warning "Error Logging message : $_.Exception.Message" 
            }
}

function SQLDoc-MissingIndexes-Collect
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [System.IO.FileInfo]
        $ServerList,
 [Parameter(Mandatory = $false)]
        [int]
        $TopN=20
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

    if ($ServerList)
        { SQLDoc-Common-CheckCollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -ServerList $ServerList 
        }
        else
        { SQLDoc-Common-CheckCollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase 
        }

if ($TopN -eq 0){
   $par1=" "}
   else{
   $par1=" top {0} " -f $TopN }

$Query="
Declare @ServerStart DateTime
select @ServerStart=sqlserver_start_time from sys.dm_os_sys_info

Select {0}
    @@ServerName as ServerName
	,SERVERPROPERTY('Edition') as [ServerEdition]
	,SERVERPROPERTY('EngineEdition') as [EngineEdition]
    ,CAST(CURRENT_TIMESTAMP AS [smalldatetime]) AS [Collection_Time]
	,@ServerStart as Sqlserver_start_time 
    ,db.[name] AS [DatabaseName]
	,id.[statement] AS [FullyQualifiedObjectName]
    ,id.[object_id] AS [ObjectID]
	,OBJECT_SCHEMA_NAME (id.[object_id], db.[database_id]) AS [SchemaName]
	,OBJECT_NAME(id.[object_id], db.[database_id]) AS [TableName]
    ,id.[equality_columns] AS [EqualityColumns]
    ,id.[inequality_columns] AS [InEqualityColumns]
    ,id.[included_columns] AS [IncludedColumns]
	,((gs.[user_seeks]+gs.[user_scans]) * gs.[avg_total_user_cost] * gs.[avg_user_impact]) AS [Impact]
    ,gs.[user_seeks] AS [UserSeeks]
    ,gs.[user_scans] AS [UserScans]
	,gs.[unique_compiles] AS [UniqueCompiles]
    ,gs.[avg_total_user_cost] AS [AvgTotalUserCost]  
    ,gs.[avg_user_impact] AS [AvgUserImpact] 
	,gs.[last_user_seek] AS [LastUserSeekTime]
    ,gs.[last_user_scan] AS [LastUserScanTime]
    ,gs.[system_seeks] AS [SystemSeeks]
    ,gs.[system_scans] AS [SystemScans]
    ,gs.[last_system_seek] AS [LastSystemSeekTime]
    ,gs.[last_system_scan] AS [LastSystemScanTime]
    ,gs.[avg_total_system_cost] AS [AvgTotalSystemCost]
    ,gs.[avg_system_impact] AS [AvgSystemImpact]
	,IIF(CharIndex(',',IsNull(id.[included_columns],''),1)=0,0, (Len(IsNull(id.[included_columns],'')) - Len(replace(IsNull(id.[included_columns],''), ',', '')))+1)  AS numberofIncludedFields
	,CONVERT(NVARCHAR(32),HASHBYTES('MD5', CONCAT(id.[statement],'=', IsNull(id.[equality_columns],'NULL'),'-',IsNULL(id.[inequality_columns], 'NULL'),'-',ISNULL(id.[included_columns], 'NULL'))),2)  as ProposedIndex_Hash
	,'CREATE INDEX [IX_' + OBJECT_NAME(id.[object_id], db.[database_id]) + '_' + REPLACE(REPLACE(REPLACE(ISNULL(id.[equality_columns], ''), ', ', '_'), '[', ''), ']', '') + CASE
        WHEN id.[equality_columns] IS NOT NULL
            AND id.[inequality_columns] IS NOT NULL
            THEN '_'
        ELSE ''
        END + REPLACE(REPLACE(REPLACE(ISNULL(id.[inequality_columns], ''), ', ', '_'), '[', ''), ']', '') + '_' + LEFT(CAST(NEWID() AS [nvarchar](64)), 5) + ']' + ' ON ' + id.[statement] + ' (' + ISNULL(id.[equality_columns], '') + CASE
        WHEN id.[equality_columns] IS NOT NULL
            AND id.[inequality_columns] IS NOT NULL
            THEN ','
        ELSE ''
        END + ISNULL(id.[inequality_columns], '') + ')' + ISNULL(' INCLUDE (' + id.[included_columns] + ')', '') AS [ProposedIndex]
FROM [sys].[dm_db_missing_index_group_stats] gs WITH (NOLOCK)
INNER JOIN [sys].[dm_db_missing_index_groups] ig WITH (NOLOCK) ON gs.[group_handle] = ig.[index_group_handle]
INNER JOIN [sys].[dm_db_missing_index_details] id WITH (NOLOCK) ON ig.[index_handle] = id.[index_handle]
INNER JOIN [sys].[databases] db WITH (NOLOCK) ON db.[database_id] = id.[database_id]
WHERE db.[database_id] > 4  
ORDER BY ((gs.[user_seeks]+gs.[user_scans]) * gs.[avg_total_user_cost] * gs.[avg_user_impact]) DESC" -f $Par1 

   # $srvrs=get-content -Path $ServerList
   Write-Host "[$srv] initiating missing index collection" -ForegroundColor Yellow

    $scon = New-Object System.Data.SqlClient.SqlConnection
    $scon.ConnectionString = "Data Source=$DataWarehouseServer;Initial Catalog=$DataWarehouseDatabase;Integrated Security=true"
        
    $cmd = New-Object System.Data.SqlClient.SqlCommand
    $cmd.Connection = $scon
    $cmd.CommandTimeout = 0
    $scon.Open()
         
    $srvrs= Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "Select ServerName from [Target_Servers] where Is_Enabled=1"
    
    foreach($row in $srvrs)
    {
        $srv =$row.Item('ServerName')

        try
        {
            $mi= Invoke-Sqlcmd -Query $Query -ServerInstance $srv -ErrorAction Stop
            Write-Host "[$srv] connected.. " -ForegroundColor Green

        $rowCount=0
        foreach ($row in $mi)
        {

            $P_01=    $row.Item('ServerName')
            $P_02=    $row.Item('ServerEdition')
            $P_03=    $row.Item('EngineEdition')
            $P_04=    $row.Item('Collection_Time')
            $P_05=    $row.Item('Sqlserver_start_time')
            $P_06=    $row.Item('DatabaseName')  
            $P_07=    $row.Item('FullyQualifiedObjectName')
            $P_08=    $row.Item('ObjectID')
            $P_09=    $row.Item('SchemaName')
            $P_10=    $row.Item('TableName')
            $P_11=    $row.Item('EqualityColumns')
            $P_12=    $row.Item('InEqualityColumns')
            $P_13=    $row.Item('IncludedColumns')
            $P_14=    $row.Item('Impact')
            $P_15=    $row.Item('UserSeeks')
            $P_16=    $row.Item('UserScans')
            $P_17=    $row.Item('UniqueCompiles')
            $P_18=    $row.Item('AvgTotalUserCost')
            $P_19=    $row.Item('AvgUserImpact')
            $P_20=    $row.Item('LastUserSeekTime')
            $P_21=    $row.Item('LastUserScanTime')
            $P_22=    $row.Item('SystemSeeks')
            $P_23=    $row.Item('SystemScans')
            $P_24=    $row.Item('LastSystemSeekTime')
            $P_25=    $row.Item('LastSystemScanTime')
            $P_26=    $row.Item('AvgTotalSystemCost')
            $P_27=    $row.Item('AvgSystemImpact')
            $P_28=    $row.Item('numberofIncludedFields')
            $P_29=    $row.Item('ProposedIndex_Hash')
            $P_30=    $row.Item('ProposedIndex')

            try
            {
                $cmd.CommandText = "EXEC dbo.Missing_Index_Manage '$P_01','$P_02',$P_03,'$P_04','$P_05','$P_06','$P_07',$P_08,'$P_09','$P_10','$P_11','$P_12','$P_13',$P_14,$P_15,$P_16,$P_17,$P_18,$P_19,'$P_20','$P_21',$P_22,$P_23,'$P_24','$P_25',$P_26,$P_27,$P_28,'$P_29','$P_30'"
                $cmd.ExecuteNonQuery() | Out-Null
                $rowCount=$rowCount+1
            }
            catch [Exception] 
            {
                Write-Warning $_.Exception.Message
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Missing index [1]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null                  
            }
        }
        
        Write-Host "[$srv] wrote $rowCount row(s)" -ForegroundColor Green
        }
        catch [Exception]
        {
                Write-Warning "Problem connecting $srv"
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Missing index [2]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null
        }

        Write-Host ""
    } 

    $scon.Close()
    $scon.Dispose()
    $cmd.Dispose()

    SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Missing index collected!'

    SQLDoc-MissingIndexes-CollectAdditionalInfo -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase
}

function SQLDoc-MissingIndexes-CollectAdditionalInfo
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [switch]
        $FullRefresh
         
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

if (!$PSBoundParameters.ContainsKey('FullRefresh'))
    {$ScanQry="select distinct Servername, DatabaseName, ObjectID  from [dbo].[Missing_Indexes] where Total_Rows is Null"}
    else
    {$ScanQry="select distinct Servername, DatabaseName, ObjectID  from [dbo].[Missing_Indexes]"}

$Qry="SELECT tbl.name as TableName,
	ind.Num_ind,
	col.Num_col,
	part.Num_Rows
FROM  
(select  object_id,name from sys.tables UNION select object_id,name from sys.views) tbl 
INNER JOIN  
(Select object_id,Count(index_id) as Num_ind from sys.indexes group by object_id ) ind on ind.object_id=Tbl.object_id
INNER JOIN  
(Select object_id, Count(column_id) as Num_col from sys.Columns  group by object_id ) col on  col.object_id=tbl.object_id
INNER JOIN  
(select Object_id, Sum(rows) as Num_Rows from sys.partitions where index_id <2 group by object_id) part on  part.object_id=tbl.object_id
WHERE tbl.object_id = "  

    $dist_obj= Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $ScanQry

    foreach($row in $dist_obj)
    {
       $step_Srv=$row.Item('ServerName')
       $step_Db=$row.Item('DatabaseName')
       $step_Id=$row.Item('ObjectID')

        try
        {
            $extraInfo= Invoke-Sqlcmd -ServerInstance $step_Srv -Database $step_Db -Query ($Qry  + ($step_Id))

            try
            {
                $var_rows=$extraInfo.Num_Rows
                $var_inds=$extraInfo.Num_Ind
                $var_cols=$extraInfo.Num_Col

                $upd_qry = "Update [Missing_Indexes] set [Total_Rows]=$var_rows ,[Total_Indexes]=$var_inds, [Total_Columns]=$var_cols where Servername='$step_Srv' and DatabaseName='$step_Db' and ObjectID=$step_Id"

                Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $upd_qry
            }
            catch [Exception] 
            {
                Write-Warning $_.Exception.Message
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Missing Index additional [1]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null 
            }
        }
        catch [Exception]
        {
                Write-Warning $_.Exception.Message
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Missing Index additional [2]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null 
        }
     } 

     Write-Host "Additional info collected" -ForegroundColor Green
     SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Missing index additional info collected!'

}

Function SQLDoc-MissingIndexes-ValidateIndex
{
param(
[Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $true)]
        [String]
        $ServerName,
 [Parameter(Mandatory = $true)]
        [String]
        $DBName,
 [Parameter(Mandatory = $true)]
        [String]
        $TableName,
 [Parameter(Mandatory = $true)]
        [String]
        $Object_ID,
 [Parameter(Mandatory = $false)]
        [String]
        $EqualityColumns,
 [Parameter(Mandatory = $false)]
        [String]
        $InEqualityColumns,
 [Parameter(Mandatory = $false)]
        [String]
        $IncludedColumns,
 [Parameter(Mandatory = $false)]
        [String]
        $Ind_Hash,
 [Parameter(Mandatory = $false)]
        [String]
        $CreateQuery
                      
)

        $TotFound=0
        $TotMI=0

    $Indexes_Found_Tbl = New-Object System.Data.DataTable
   
        $Indexes_Found_Tbl.Columns.Add(“table_name”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“index_name”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“index_description”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“indexed_columns”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“included_columns”, "System.String") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“usage_scans”, "System.Int32") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“usage_seeks”, "System.Int32") | Out-Null
        $Indexes_Found_Tbl.Columns.Add(“usage_lookups”, "System.Int32") | Out-Null


##Collect existing indexes with usage

    $sqlDB = "SELECT '[' + sch.NAME + '].[' + obj.NAME + ']' AS 'table_name'
    ,+ ind.NAME AS 'index_name'
    ,LOWER(ind.type_desc) + CASE 
        WHEN ind.is_unique = 1
            THEN ', unique'
        ELSE ''
        END + CASE 
        WHEN ind.is_primary_key = 1
            THEN ', primary key'
        ELSE ''
        END AS 'index_description'
    ,STUFF((
            SELECT ', [' + sc.NAME + ']' AS ""text()""
            FROM sys.columns AS sc
            INNER JOIN sys.index_columns AS ic ON ic.object_id = sc.object_id
                AND ic.column_id = sc.column_id
            WHERE sc.object_id = obj.object_id
                AND ic.index_id = ind.index_id
                AND ic.is_included_column = 0
            ORDER BY key_ordinal
            FOR XML PATH('')
            ), 1, 2, '') AS 'indexed_columns'
    ,STUFF((
            SELECT ', [' + sc.NAME + ']' AS ""text()""
            FROM sys.columns AS sc
            INNER JOIN sys.index_columns AS ic ON ic.object_id = sc.object_id
                AND ic.column_id = sc.column_id
            WHERE sc.object_id = obj.object_id
                AND ic.index_id = ind.index_id
                AND ic.is_included_column = 1
            FOR XML PATH('')
            ), 1, 2, '') AS 'included_columns',
            ISNULL(usage.user_seeks,0) as seeks_count,
			ISNULL(usage.user_scans,0) as scans_count,
			ISNULL(usage.user_lookups,0) as lookups_count
FROM sys.indexes AS ind 
INNER JOIN sys.objects AS obj ON ind.object_id = obj.object_id
    AND obj.is_ms_shipped = 0
INNER JOIN sys.schemas AS sch ON sch.schema_id = obj.schema_id
left outer JOIN sys.dm_db_index_usage_stats AS usage ON ind.object_id = usage.object_id and ind.index_id=usage.index_id
WHERE obj.type = 'U'
    AND ind.type in (1,2)
    AND obj.NAME <> 'sysdiagrams'
	and ind.object_id=$Object_ID
ORDER BY ind.index_id"

    $IndexesFound = Invoke-Sqlcmd -ServerInstance $ServerName -Database $DBName -Query $sqlDB 
    

    
    if ($InEqualityColumns -eq '')
        {$cols=$EqualityColumns}
    elseif ($EqualityColumns -eq '')
        {$cols=$InEqualityColumns}    
    else
        {$cols=$EqualityColumns + ',' +$InEqualityColumns }
    

    foreach($indRow in $IndexesFound)
    {

        $iRow = $Indexes_Found_Tbl.NewRow()
            $iRow[“table_name”] = $indRow.Item('table_name')
            $iRow[“index_name”] = $indRow.Item('index_name')
            $iRow[“index_description”] = $indRow.Item('index_description')
            $iRow[“indexed_columns”] = $indRow.Item('indexed_columns')
            $iRow[“included_columns”] = $indRow.Item('included_columns')
            $iRow[“usage_scans”] = $indRow.Item('scans_count')
            $iRow[“usage_seeks”] = $indRow.Item('seeks_count')
            $iRow[“usage_lookups”] = $indRow.Item('lookups_count')

        $Indexes_Found_Tbl.rows.Add($iRow)


 
        if ($indRow.Item('indexed_columns') -eq  $cols)
        {
            $msg="identical index " + $indRow.Item('index_name') + " Found!"
            Write-Host $msg -ForegroundColor Yellow
            Read-Host “Press ENTER to continue...”
            #Write-Host -NoNewLine 'Press any key to continue...';
            #$null = $Host.UI.RawUI.ReadKey('NoEcho,IncludeKeyDown');
            Return
        } 
         
       $TotFound=$TotFound+1
    }
    
##Collect missing indexes from catalog
 
    $sqlMI = "select 
 SUBSTRING(FullyQualifiedObjectName, CHARINDEX ( '.' , FullyQualifiedObjectName  )+1,Len(FullyQualifiedObjectName)-CHARINDEX ( '.' , FullyQualifiedObjectName  )) as Table_Name,
'*' as index_name,
'Missing Index' as index_description,
case When ([InEqualityColumns]='') then [EqualityColumns]
	 When ([EqualityColumns]='') then [InEqualityColumns]
     ELSE [EqualityColumns] +'.' + [InEqualityColumns]
   END as indexed_columns,   
IncludedColumns as  included_columns
from [dbo].[Missing_Indexes]
where ServerName='$ServerName'
and DatabaseName = '$DBName'
and OBJECTID = '$Object_ID'
and [ProposedIndex_Hash] <> '$Ind_Hash'
ORDER BY indexed_columns"

    $IndexesFound2 = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $sqlMI 
    
    
    foreach($indRow2 in $IndexesFound2)
    {

        $iRow = $Indexes_Found_Tbl.NewRow()
            $iRow[“table_name”] = $indRow2.Item('table_name')
            $iRow[“index_name”] = $indRow2.Item('index_name')
            $iRow[“index_description”] = $indRow2.Item('index_description')
            $iRow[“indexed_columns”] = $indRow2.Item('indexed_columns')
            $iRow[“included_columns”] = $indRow2.Item('included_columns')
            $iRow[“usage_scans”] = 0
            $iRow[“usage_seeks”] = 0
            $iRow[“usage_lookups”] = 0
        $Indexes_Found_Tbl.rows.Add($iRow)

       $TotMI=$TotMI+1
    }

    

    If (($TotFound + $TotMI) -gt 0)
    {
        #Write-Host ($TotFound + $TotMI) -ForegroundColor Green 
         $Indexes_Found_Tbl | Format-Table
    }
    else
    {
       Write-Host "No existing and missing indexes found on table" -ForegroundColor Green 
    }
   

    Write-host "Would you like to create Missing index? (Default is No)" -ForegroundColor Yellow 
    
    $Readhost = Read-Host " ( y / n ) " 
    Switch ($ReadHost) 
     { 
       Y {Write-host "Yes, create index"; $Answ=$true} 
       N {Write-Host "No, skip this missing index"; $Answ=$false} 
       Default {Write-Host "Default, skip this missing index"; $Answ=$false}  
     } 

    IF ($Answ)
        {
            try
            {
                Invoke-Sqlcmd -ServerInstance $ServerName -Database $DBName -Query $CreateQuery
                Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "delete from [dbo].[Missing_Indexes] where [ProposedIndex_Hash]='$Ind_Hash'"
                $LogMsg0='User added ' + $iRow[“index_name”] + ' to ' + $DBName + ' database on ' + $ServerName + ' Server'
                SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message $LogMsg0
            }
            catch [Exception] #exec each row
            {
                Write-Warning $_.Exception.Message
                $cmd1 = "INSERT INTO Collection_Errors (ServerName,Error_Message) VALUES ('$Srv','$_.Exception.Message')"
                Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $cmd1                  
            }
        }
} 

function SQLDoc-MissingIndexes-Create
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [String]
        $TargetServer,
 [Parameter(Mandatory = $false)]
        [String]
        $TargetDatabase,
 [Parameter(Mandatory = $false)]
        [int]
        $TopN=20,
 [Parameter(Mandatory = $false)]
        [int]
        $Minimal_Impact=0 
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

if ($TopN -eq 0){
   $par1=" "}
   else{$par1=" top {0} " -f $TopN }

if ($TargetServer -eq ''){
   $par2=" "}
   else{$par2=" and ([ServerName]='{0}') " -f $TargetServer }

if ($TargetDatabase -eq ''){
   $par3=" "}
   else{$par3=" and ([DatabaseName]='{0}') " -f $TargetDatabase }
   
$Query="Select {0}
* from [dbo].[Missing_Indexes]
where Impact > {1}{2}{3} 
order by Impact DESC" -f $Par1,$Minimal_Impact,$Par2,$Par3

  $IndexesFound = Invoke-Sqlcmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $Query

  foreach ($rowFound in $IndexesFound)
  {
    $rowFound

    MissingIndexes-ValidateIndex -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase   -ServerName $rowFound.Item('ServerName') -DBName $rowFound.Item('DatabaseName') -TableName $rowFound.Item('TableName') -Object_ID $rowFound.Item('ObjectID') -EqualityColumns $rowFound.Item('EqualityColumns') -InEqualityColumns $rowFound.Item('InEqualityColumns') -IncludedColumns $rowFound.Item('IncludedColumns') -CreateQuery $rowFound.Item('ProposedIndex') -Ind_Hash $rowFound.Item('ProposedIndex_Hash') 
   }
}

function SQLDoc-Jobs-Collect
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

    SQLDoc-Common-CheckCollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase 


$Query="SELECT
@@ServerName as ServerName,
(select status_desc from sys.dm_server_services where servicename = 'SQL Server Agent (MSSQLSERVER)' ) as agentsvc_status,
(select startup_type_desc   from sys.dm_server_services where servicename = 'SQL Server Agent (MSSQLSERVER)' ) as agentsvc_startupType,
j.name as JobName,
j.enabled as Is_enabled ,
sc.name as 	Schedule_Name, 
IsNull(sc.enabled,'0')	as Schedule_enabled,
CASE
	WHEN ja.job_id IS NOT NULL AND ja.stop_execution_date IS NULL THEN 1 ELSE 0 
END as    IsRunning,
--ja.run_requested_source  as   Request,Source,
--IsNull(ja.run_requested_source,0) as RequestSource,
NULL as RequestSource,
ja.start_execution_date as     LastRunTime,
ja.next_scheduled_run_date as     NextRunTime,
Replace(js.step_name,'''','')  as     LastJobStep,
IsNull(jh.retries_attempted,0) as     RetryAttempt,
CASE
	WHEN ja.job_id IS NOT NULL
		AND ja.stop_execution_date IS NULL THEN 'Running'
	WHEN jh.run_status = 0 THEN 'Failed'
	WHEN jh.run_status = 1 THEN 'Succeeded'
	WHEN jh.run_status = 2 THEN 'Retry'
	WHEN jh.run_status = 3 THEN 'Cancelled'
END as     JobLastOutcome ,
IsNull(jh.run_duration,0) as    JobLastDuration ,
IsNull(last7.TotalRunsLast7Days,0) as TotalRunsLast7Days,
IsNull(last7.FailedRunsLast7Days,0) as FailedRunsLast7Days,
IsNull(last7.AverageDuration,0)  as AverageDuration,
IsNull(last7.MaxDuration,0) as MaxDuration,
GetDate() as Collection_Time
FROM msdb.dbo.sysjobs j
left outer join msdb.dbo.sysjobschedules sjs on j.job_id = sjs.job_id
left outer join msdb.dbo.sysschedules sc on sjs.schedule_id = sc.schedule_id 
LEFT JOIN msdb.dbo.sysjobactivity ja 
    ON ja.job_id = j.job_id
       AND ja.run_requested_date IS NOT NULL
       AND ja.start_execution_date IS NOT NULL
LEFT JOIN msdb.dbo.sysjobsteps js
    ON js.job_id = ja.job_id
       AND js.step_id = ja.last_executed_step_id
LEFT JOIN msdb.dbo.sysjobhistory jh
    ON jh.job_id = j.job_id
       AND jh.instance_id = ja.job_history_id
--WHERE j.name = @JobName
LEFT JOIN (sELECT 
	job_id, 
	count(h.run_date) 'TotalRunsLast7Days', --h.run_duration
	Sum(CASE WHEN h.run_status='0' and h.step_id = '0'THEN 1 else 0 END) as 'FailedRunsLast7Days',
	Avg (h.run_duration) as AverageDuration,
	Max (h.run_duration) as MaxDuration 
FROM 
 msdb.dbo.sysjobhistory h 
WHERE 
	--j.enabled = '1' and h.run_status = '0' and 
	h.step_id = '0'
	and h.run_date >= CONVERT(VARCHAR(8), GETDATE(), 112) - 30 -- past 7 days
GROUP BY
	job_id) as last7 on j.job_id=last7.job_id" 

    Write-Host "[$srv] initiating jobs collection" -ForegroundColor Yellow

    $scon = New-Object System.Data.SqlClient.SqlConnection
    $scon.ConnectionString = "Data Source=$DataWarehouseServer;Initial Catalog=$DataWarehouseDatabase;Integrated Security=true"
        
    $cmd = New-Object System.Data.SqlClient.SqlCommand
    $cmd.Connection = $scon
    $cmd.CommandTimeout = 0
    $scon.Open()
         
    $srvrs= Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "Select ServerName from [Target_Servers] where Is_Enabled=1"
     
    foreach($row in $srvrs)
    {
        $srv =$row.Item('ServerName')
        try
        {
            $mi= Invoke-Sqlcmd -Query $Query -ServerInstance  $srv -Database 'msdb' -ErrorAction Stop
            Write-Host "[$srv] connected.. " -ForegroundColor Green

        $rowCount=0
        foreach ($row in $mi)
        {

            $P_01=    $row.Item('ServerName')
            $P_02=    $row.Item('agentsvc_status')
            $P_03=    $row.Item('agentsvc_startupType')
            $P_04=    $row.Item('JobName')
            $P_05=    $row.Item('Is_enabled')
            $P_06=    $row.Item('Schedule_Name')  
            $P_07=    $row.Item('Schedule_enabled')
            $P_08=    $row.Item('IsRunning')
            $P_09=    $row.Item('RequestSource')
            $P_10=    $row.Item('LastRunTime')
            $P_11=    $row.Item('NextRunTime')
            $P_12=    $row.Item('LastJobStep')
            $P_13=    $row.Item('RetryAttempt')
            $P_14=    $row.Item('JobLastOutcome')
            $P_15=    $row.Item('JobLastDuration')
            $P_16=    $row.Item('TotalRunsLast7Days')
            $P_17=    $row.Item('FailedRunsLast7Days')
            $P_18=    $row.Item('AverageDuration')
            $P_19=    $row.Item('MaxDuration')
            #$P_20=    $row.Item('Collection_Time')

            try
            {
                $cmd.CommandText = "EXEC dbo.JobInfo_Manage '$P_01','$P_02','$P_03','$P_04',$P_05,'$P_06',$P_07,$P_08,'$P_09','$P_10','$P_11','$P_12',$P_13,'$P_14',$P_15,$P_16,$P_17,$P_18,$P_19"
                $cmd.ExecuteNonQuery() | Out-Null
                $rowCount=$rowCount+1
            }
            catch [Exception] 
            {
                Write-Warning $_.Exception.Message
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Job collection [1]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null                  
            }
        }
        
        Write-Host "[$srv] wrote $rowCount row(s)" -ForegroundColor Green
        }
        catch [Exception]
        {
                Write-Warning "Problem connecting $srv"
                #Write-Warning  $cmd.CommandText
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Job Collection [2]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null
        }

        Write-Host ""
    } 

    $scon.Close()
    $scon.Dispose()
    $cmd.Dispose()

    SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Job Info collected!'
}

function SQLDoc-PerfCounters-Collect
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

    SQLDoc-Common-CheckCollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase 


$Query="SELECT @@ServerName as ServerName,
GetDate() as currentTime,
[object_name],[counter_name], [cntr_type], [instance_name],[cntr_value]
 FROM sys.dm_os_performance_counters
WHERE [counter_name] in( 'Available MBytes',
'% Processor Time',
'Forwarded Records/sec',
'Full scans/sec',
'Page Splits / Sec',
'Buffer Cache hit ratio',
'Checkpoint Pages / Sec',
'Page life expectancy',
'User Connections',
--'Average Wait Time (ms)',
'Lock Waits / Sec',
'Memory Grants Pending',
'Target Server Memory (KB)',
'Total Server Memory (KB)',
'Batch Requests/Sec',
'SQL Compilations/Sec',
'SQL Re-Compilations/Sec')
--and instance_name = ''" 

    Write-Host "[$srv] initiating PerfMon collection" -ForegroundColor Yellow

    $scon = New-Object System.Data.SqlClient.SqlConnection
    $scon.ConnectionString = "Data Source=$DataWarehouseServer;Initial Catalog=$DataWarehouseDatabase;Integrated Security=true"
        
    $cmd = New-Object System.Data.SqlClient.SqlCommand
    $cmd.Connection = $scon
    $cmd.CommandTimeout = 0
    $scon.Open()
         
    $srvrs= Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "Select ServerName from [Target_Servers] where Is_Enabled=1"
     
    foreach($row in $srvrs)
    {
        $srv =$row.Item('ServerName')
        try
        {
            $mi= Invoke-Sqlcmd -Query $Query -ServerInstance  $srv -Database 'master' -ErrorAction Stop -QueryTimeout 5
            Write-Host "[$srv] connected.. " -ForegroundColor Green

        $rowCount=0
        foreach ($row in $mi)
        {

            $P_01=    $row.Item('ServerName')
            $P_02=    $row.Item('currentTime')
            $P_03=    $row.Item('object_name')
            $P_04=    $row.Item('counter_name')
            $P_05=    $row.Item('cntr_type')
            $P_06=    $row.Item('instance_name')  
            $P_07=    $row.Item('cntr_value')

            try
            {
                $cmd.CommandText = "EXEC dbo.PerfMon_Manage '$P_01','$P_02','$P_03','$P_04',$P_05,'$P_06',$P_07"
                $cmd.ExecuteNonQuery() | Out-Null
                $rowCount=$rowCount+1
            }
            catch [Exception] 
            {
                Write-Warning $_.Exception.Message
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','PerfMon collection [1]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null                  
            }
        }
        
        Write-Host "[$srv] wrote $rowCount row(s)" -ForegroundColor Green
        }
        catch [Exception]
        {
                Write-Warning "Problem connecting $srv"
                #Write-Warning  $cmd.CommandText
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','PerfMon Collection [2]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null
        }

        Write-Host ""
    } 

    $scon.Close()
    $scon.Dispose()
    $cmd.Dispose()

    SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'PerfMon Info collected!'
}

function SQLDoc-Backups-Collect
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

    SQLDoc-Common-CheckCollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase 


$Query="select @@Servername as ServerName, dbs.name as DBName ,dbs.recovery_model_Desc as recovery_model,
    mf.logSize_mb , mf.rowSize_mb , mf.totalSize_mb, 
    ISNULL(bck.FullBck,0) as FullBck_count ,ISNULL(bck.LogBck,0) as LogBck_count , ISNULL(bck.OtherBck,0) as OtherBck_count , bck.LastFullBackup , bck.LastLogBackup , ISNULL(bck.MaxFullDuration,0) as MaxFullDuration , ISNULL(bck.MaxLogDuration,0) as MaxLogDuration, ISNULL(bck.MaxBackupSize_mb,0) as MaxBackupSize_mb , GetDate() as Collection_Date  
from sys.databases dbs
Left Outer Join
(SELECT bs.database_name,
Sum(CASE WHEN bs.type = 'D' THEN 1 ELSE 0 END) AS FullBck,
Sum(CASE WHEN bs.type = 'L' THEN 1 ELSE 0 END) AS LogBck,
Sum(CASE WHEN bs.type in  ( 'I', 'F','G','P','Q')  THEN 1 ELSE 0 END) AS OtherBck, 
Max(CASE WHEN bs.type = 'D' THEN bs.backup_start_date ELSE NULL END) as LastFullBackup,
Max(CASE WHEN bs.type = 'L' THEN bs.backup_start_date ELSE NULL END) as LastLogBackup,
Max(CASE WHEN bs.type = 'D' THEN DATEDIFF(second, bs.backup_start_date, bs.backup_finish_date) ELSE 0 END) as MaxFullDuration ,
Max(CASE WHEN bs.type = 'L' THEN DATEDIFF(second, bs.backup_start_date, bs.backup_finish_date) ELSE 0 END) as MaxLogDuration ,
Max(Cast ( bs.backup_size / 1024 /1024  as  BIGINT  )) as MaxBackupSize_MB
 from msdb.dbo.backupset bs WITH (NoLock) 
WHERE (CONVERT(datetime, bs.backup_start_date, 102) >= GETDATE() - 30)
Group BY 
bs.database_name) bck on dbs.name = bck.database_name
Left outer Join ( 
SELECT 
	DB_NAME(database_id) as dbName,
    CAST(SUM(CASE WHEN type_desc = 'LOG' THEN size END) * 8. / 1024 AS DECIMAL(8,2)) as logSize_mb , 
    CAST(SUM(CASE WHEN type_desc = 'ROWS' THEN size END) * 8. / 1024 AS DECIMAL(8,2)) as  rowSize_mb , 
    CAST(SUM(size) * 8. / 1024 AS DECIMAL(8,2)) as totalSize_mb 
FROM sys.master_files WITH(NoLock)
GROUP BY database_id ) as mf on dbs.name=mf.dbName
where dbs.name not in ('TempDB')" 

    Write-Host "[$srv] initiating backup collection" -ForegroundColor Yellow

    $scon = New-Object System.Data.SqlClient.SqlConnection
    $scon.ConnectionString = "Data Source=$DataWarehouseServer;Initial Catalog=$DataWarehouseDatabase;Integrated Security=true"
        
    $cmd = New-Object System.Data.SqlClient.SqlCommand
    $cmd.Connection = $scon
    $cmd.CommandTimeout = 0
    $scon.Open()
   
    $srvrs= Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "Select ServerName from [Target_Servers] where Is_Enabled=1"
    foreach($row in $srvrs)
    {
        $srv =$row.Item('ServerName')
        try
        {
            $mi= Invoke-Sqlcmd -Query $Query -ServerInstance  $srv -Database 'msdb' -ErrorAction Stop
            Write-Host "[$srv] connected.. " -ForegroundColor Green

        $rowCount=0
        foreach ($row in $mi)
        {
            $P_01=    $row.Item('ServerName')
            $P_02=    $row.Item('DBname')
            $P_03=    $row.Item('recovery_Model')
            $P_04=    $row.Item('logSize_mb')
            $P_05=    $row.Item('rowSize_mb')
            $P_06=    $row.Item('totalSize_mb')  
            $P_07=    $row.Item('FullBck_count')
            $P_08=    $row.Item('LogBck_count')
            $P_09=    $row.Item('OtherBck_count')
            $P_10=    $row.Item('LastFullBackup')
            $P_11=    $row.Item('LastLogBackup')
            $P_12=    $row.Item('MaxFullDuration')
            $P_13=    $row.Item('MaxLogDuration')
            $P_14=    $row.Item('MaxBackupSize_mb')
            $P_15=    $row.Item('Collection_Date')
            try
            {
                $cmd.CommandText = "EXEC dbo.BackupInfo_Manage '$P_01','$P_02','$P_03',$P_04,$P_05,$P_06,$P_07,$P_08,$P_09,'$P_10','$P_11',$P_12,$P_13,$P_14,'$P_15'"
                $cmd.ExecuteNonQuery() | Out-Null
                $rowCount=$rowCount+1
            }
            catch [Exception] 
            {
                Write-Warning $_.Exception.Message
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Backup collection [1]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null                  
            }
        }
        
        Write-Host "[$srv] wrote $rowCount row(s)" -ForegroundColor Green
        }
        catch [Exception]
        {
                Write-Warning "Problem connecting $srv"
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Backup Collection [2]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null
        }

        Write-Host ""
    } 

    $scon.Close()
    $scon.Dispose()
    $cmd.Dispose()

    SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Backup Info collected!'
}

function SQLDoc-BadLogins-Collect
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $true)]
        [System.IO.FileInfo]
        $PasswordListPath
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}

    SQLDoc-Common-CheckCollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase 

    $scon = New-Object System.Data.SqlClient.SqlConnection
    $scon.ConnectionString = "Data Source=$DataWarehouseServer;Initial Catalog=$DataWarehouseDatabase;Integrated Security=true"
        
    $cmd = New-Object System.Data.SqlClient.SqlCommand
    $cmd.Connection = $scon
    $cmd.CommandTimeout = 0
    $scon.Open()
         
    $srvrs= Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "Select ServerName from [Target_Servers] where Is_Enabled=1"
    $pwdList = Get-Content $PasswordListPath 
     
    foreach($row in $srvrs)
    {
        $srv =$row.Item('ServerName')
        try
        {
        
            $QueryMain ="SELECT @@ServerName as ServerName, name, 'Same name as Password' as descr FROM sys.sql_logins WHERE PWDCOMPARE(name, password_hash) = 1
                         Union ALL 
                         SELECT @@ServerName as ServerName, name, 'blank Password' as descr FROM sys.sql_logins WHERE PWDCOMPARE('', password_hash) = 1"
            $mi= Invoke-Sqlcmd -Query $QueryMain -ServerInstance  $srv -Database 'master' -ErrorAction Stop

            Write-Host "[$srv] connected.. " -ForegroundColor Green

            foreach ($row in $mi)
            {
                $P_01=    $row.Item('ServerName')
                $P_02=    $row.Item('Name')
                $P_03=    $row.Item('descr')

                Write-Host " issue found with $P_02 on $srv [1]"

                try
                {
                    $cmd.CommandText = "EXEC dbo.BadLoginInfo_Manage '$P_01','$P_02','$P_03'"
                    $cmd.ExecuteNonQuery() | Out-Null
                    $rowCount=$rowCount+1
                }
                catch [Exception] 
                {
                    Write-Warning $_.Exception.Message
                    $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Bad Login collection [1]','$_.Exception.Message')"
                    $cmd.ExecuteNonQuery() | Out-Null                  
                }
            }

            foreach ($password in $pwdList)
            {
                $QueryPwd = "SELECT @@ServerName as ServerName, name, 'weak password [$password]'  as descr FROM sys.sql_logins WHERE PWDCOMPARE('$password', password_hash) = 1 ;" 
                $mi= Invoke-Sqlcmd -Query $QueryPwd -ServerInstance  $srv -Database 'master' -ErrorAction Stop
                            foreach ($row in $mi)
                {
                    $P_01=    $row.Item('ServerName')
                    $P_02=    $row.Item('Name')
                    $P_03=    $row.Item('descr')

                    Write-Host " issue found with $P_02 on $srv [2]"
                    
                    try
                    {
                        $cmd.CommandText = "EXEC dbo.BadLoginInfo_Manage '$P_01','$P_02','$P_03'"
                        $cmd.ExecuteNonQuery() | Out-Null
                        $rowCount=$rowCount+1
                    }
                    catch [Exception] 
                    {
                        Write-Warning $_.Exception.Message
                        $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Bad Login collection [1]','$_.Exception.Message')"
                        $cmd.ExecuteNonQuery() | Out-Null                  
                    }
                }
            }

  
        Write-Host "[$srv] wrote $rowCount row(s)" -ForegroundColor Green
        }
        catch [Exception]
        {
                Write-Warning "Problem connecting $srv"
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Bad Login Collection [2]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null
        }

        Write-Host ""
    } 

    $scon.Close()
    $scon.Dispose()
    $cmd.Dispose()

    SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Bad Login Info collected!'
}

function SQLDoc-Assessment-Collect
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase
)

    SQLDoc-Common-CheckCollectionDB -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase 
    
    $scon = New-Object System.Data.SqlClient.SqlConnection
    $scon.ConnectionString = "Data Source=$DataWarehouseServer;Initial Catalog=$DataWarehouseDatabase;Integrated Security=true"
        
    $cmd = New-Object System.Data.SqlClient.SqlCommand
    $cmd.Connection = $scon
    $cmd.CommandTimeout = 0
    $scon.Open()
         
    $srvrs= Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "Select ServerName from [Target_Servers] where Is_Enabled=1"
    
    foreach($row in $srvrs)
    {
        $srv =$row.Item('ServerName')
        try
        {
            Get-SqlInstance -ServerInstance $srv | Invoke-SqlAssessment -FlattenOutput -ErrorAction Ignore -OutVariable DataX  | Out-Null
            Get-SqlDatabase -ServerInstance $srv | Invoke-SqlAssessment -FlattenOutput -ErrorAction Ignore -OutVariable +DataX    | Out-Null
                      
            Write-Host "[$srv] connected.. " -ForegroundColor Green

            Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "Delete from [dbo].AssessmentData where ServerName='$srv'"

            $rowCount=0
            foreach ($row in $DataX)
            {
                $P_01=    $Srv
                $P_02=    $row.CheckName.Replace("'","*") 
                $P_03=    $row.CheckId 
                $P_04=    $row.RulesetName 
                $P_05=    $row.RulesetVersion 
                $P_06=    $row.Severity 
                $P_07=    $row.Message.Replace("'","*")
                $P_08=    $row.TargetPath.Replace("'","*")
                $P_09=    $row.TargetType 
                $P_10=    $row.HelpLink 

                try
                {
                    $cmd.CommandText = "EXEC dbo.AssessmentData_Manage '$srv','$P_02','$P_03','$P_04','$P_05','$P_06','$P_07','$P_08','$P_09','$P_10'"
                    $cmd.ExecuteNonQuery() | Out-Null
                    $rowCount=$rowCount+1
                }
                catch [Exception] 
                {
                    Write-Warning $_.Exception.Message
                    $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Assessment collection [1]','$_.Exception.Message')"
                    $cmd.ExecuteNonQuery() | Out-Null                  
                }
        }

        Write-Host "[$srv] wrote $rowCount row(s)" -ForegroundColor Green
        }
        catch [Exception]
        {
                Write-Warning "Problem connecting $srv"
                $cmd.CommandText = "INSERT INTO Collection_Errors (ServerName,Collection_Type, Error_Message) VALUES ('$Srv','Assessment collection [2]','$_.Exception.Message')"
                $cmd.ExecuteNonQuery() | Out-Null
        }

        Write-Host ""
    } 

    $scon.Close()
    $scon.Dispose()
    $cmd.Dispose()

    SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Assessment Data collected!'
}

function SQLDoc-Report-All
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [String]
        $ReportFolder 
)
SQLDoc-Report-IndexPage -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -ReportFolder $ReportFolder 
SQLDoc-Report-GeneralInfo -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -ReportFolder $ReportFolder -CreateSubReports
SQLDoc-Report-MissingIndexes -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -ReportFolder $ReportFolder -CreateSubReports
SQLDoc-Report-JobInfo -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -ReportFolder $ReportFolder -CreateSubReports
SQLDoc-Report-BackupInfo -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -ReportFolder $ReportFolder -CreateSubReports
SQLDoc-Report-AssessmentData -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -ReportFolder $ReportFolder -CreateSubReports
}


function SQLDoc-Collect-All
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [String]
        $ReportFolder 
)

SQLDoc-Backups-Collect -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase
SQLDoc-Jobs-Collect -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase
SQLDoc-MissingIndexes-Collect -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase
SQLDoc-Main-CollectGeneralInfo -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase
SQLDoc-PerfCounters-Collect -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase
}



function SQLDoc-Report-IndexPage
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [String]
        $ReportFolder
)


    $InfoQry="SELECT srv.[ServerName], srv.[Environment_Type],
	-- '<a href=''.\' + [ServerName] + '.htm'' target=''_blank''>' + [ServerName] + '</a>' as Link, 	
	'<a href=''.\' + srv.[ServerName] + '\SQLDoc_Server.htm''><button> Server Info </button></a>' as ServerLink,
	'<a href=''.\' + srv.[ServerName] + '\SQLDoc-AssessmentData.htm''><button> Best Practices : ' + Cast(aa.cnt as varchar(4))  + '</button></a>' as AssessmentLink,
	'<a href=''.\' + srv.[ServerName] + '\SQLDoc_missing_index.htm''><button> Missing Indexes : ' + Cast(mi.cnt as varchar(4))  + '</button></a>' as MissingIndexLink,
	'<a href=''.\' + srv.[ServerName] + '\SQLDoc-Jobs-Report.htm''><button> Jobs : ' + Cast(ji.cnt as varchar(4))  + '</button></a>' as JobInfoLink,
	'<a href=''.\' + srv.[ServerName] + '\SQLDoc-Backup-Report.htm''><button> Backup Info (last 24hrs) </button></a>' as BackupInfoLink
	FROM [dbo].[Target_Servers] srv
	left outer join (select ServerName, Count(*) as cnt from [dbo].[Missing_Indexes] group by ServerName ) mi on srv.ServerName=mi.ServerName
	left outer join (select ServerName, Count(*) as cnt from         [dbo].[JobInfo] group by ServerName ) ji on srv.ServerName=ji.ServerName
	left outer join (select ServerName, Count(*) as cnt from  [dbo].[AssessmentData] group by ServerName ) aa on srv.ServerName=aa.ServerName
	where srv.[is_Enabled]=1
	order by srv.[ServerName]"
 
    $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
    $ReportHeader ="<div class='header'><h1></h1></div>"
    $ReportFooter = "<script src=mi_script.js></script>"
    
    $HTML = $ReportData | Select ServerName, Environment_Type, ServerLink, AssessmentLink,  MissingIndexLink, JobInfoLink, BackupInfoLink | ConvertTo-Html -CSSUri mi_style.css -Title "index page"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)"  

    Add-Type -AssemblyName System.Web
    [System.Web.HttpUtility]::HtmlDecode($HTML) | Out-File -Encoding utf8 $ReportFolder\"Index.htm"

    #$ReportData

    foreach ($srv in $ReportData)
    {
     $step1=$srv.Item('ServerName')
        if(!(Test-Path -path $ReportFolder\$step1))  
            {  
                New-Item -ItemType directory -Path $ReportFolder\$step1
            }
    }
            SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Report [Index Page] created!'
 }

function SQLDoc-Report-GeneralInfo
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $false)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [String]
        $ReportFolder,
 [Parameter(Mandatory = $false)]
        [Switch]
        $CreateSubReports
)

if ($DataWarehouseDatabase -eq '')
    {$DataWarehouseDatabase='SQL_Datawarehouse'}


    $InfoQry="	select s.ServerName as Server, s.Environment_Type, i.*
	    from [dbo].[Target_Servers] s 
	    left outer join [dbo].[SQL_GeneralInfo] i on s.ServerName=i.ServerName
	    where s.[is_Enabled]=1
	    order by s.[ServerName] "


    if (!$PSBoundParameters.ContainsKey('CreateSubReports'))
        {
            $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
            $ReportHeader ="<div class='header'><h1></h1></div>"
            $ReportFooter = "<script src=mi_script.js></script>"
            #$ReportData | select ServerName , DatabaseName, Number_of_Indexes , Total_Impact  | ConvertTo-Html -CSSUri mi_style.css -Title "Missing indexes Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\"SQLDoc_Servers.htm"
            $ReportData | ConvertTo-Html -CSSUri mi_style.css -Title "Missing indexes Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\"SQLDoc_Servers.htm"
        }
    else
        {$ReportDataMain=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
            $ReportHeader ="<div class='header'><h1></h1></div>"
            #$ReportFooter = "<script src=..\mi_script.js></script>"
            $ReportFooter = "<script></script>"
 
            foreach($row in $ReportDataMain)
            {
                $srvr =$row.Item('Server')
                if(!(Test-Path -path $ReportFolder\$srvr))  
                    {  
                        New-Item -ItemType directory -Path $ReportFolder\$srvr
                    }
              #$ReportData | select ServerName , DatabaseName, Number_of_Indexes , Total_Impact  | ConvertTo-Html -CSSUri mi_style.css -Title "Missing indexes Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\"Start.htm"
              $row | ConvertTo-Html -CSSUri ..\mi_style_vert.css -Title "Missing indexes Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\$Srvr\"SQLDoc_Server.htm"
            }

        }
        SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Report [General info] created!'

}

function SQLDoc-Report-MissingIndexes
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [String]
        $ReportFolder,
 [Parameter(Mandatory = $false)]
        [Switch]
        $CreateSubReports
)

 $InfoQry="select * 
                from Missing_Indexes
                order by Impact"

    if (!$PSBoundParameters.ContainsKey('CreateSubReports'))
        {
            $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
            $ReportHeader ="<div class='header'><h1></h1></div>"
            $ReportFooter = "<script src=mi_script.js></script>"
    
            $ReportData | Select ServerName, DatabaseName, SchemaName, TableName, FullyQualifiedObjectName, Impact, Total_Rows, Total_Indexes, Total_Columns, UserSeeks, UserScans, UniqueCompiles, AvgTotalUserCost, AvgUserImpact, EqualityColumns, InEqualityColumns, IncludedColumns, ProposedIndex, ProposedIndex_Hash, ServerEdition, EngineEdition, Collection_Time, First_Detected_date, Last_Detected_date, Number_of_Detections, Sqlserver_start_time, LastUserSeekTime, LastUserScanTime, SystemSeeks, SystemScans, LastSystemSeekTime, LastSystemScanTime, AvgTotalSystemCost, AvgSystemImpact, numberofIncludedFields `
             | ConvertTo-Html -CSSUri mi_style.css -Title "Missing indexes Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\"Missing-Index-Report.htm"

        }
    else
        {
            $LoopData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "select distinct ServerName from [dbo].[Missing_Indexes]"
            foreach($row in $LoopData)
            {
                $srvr =$row.Item('ServerName')
                $InfoQry="select * from [dbo].[Missing_Indexes]
                where [ServerName]='$srvr'
                order by Impact"

                if(!(Test-Path -path $ReportFolder\$srvr))  
                    {  
                        New-Item -ItemType directory -Path $ReportFolder\$srvr
                    }

                $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
                $ReportHeader ="<div class='header'><h1></h1></div>"
                $ReportFooter = "<script src=../mi_script.js></script>"
    
                $ReportData | Select ServerName, DatabaseName, SchemaName, TableName, FullyQualifiedObjectName, Impact, Total_Rows, Total_Indexes, Total_Columns, UserSeeks, UserScans, UniqueCompiles, AvgTotalUserCost, AvgUserImpact, EqualityColumns, InEqualityColumns, IncludedColumns, ProposedIndex, ProposedIndex_Hash, ServerEdition, EngineEdition, Collection_Time, First_Detected_date, Last_Detected_date, Number_of_Detections, Sqlserver_start_time, LastUserSeekTime, LastUserScanTime, SystemSeeks, SystemScans, LastSystemSeekTime, LastSystemScanTime, AvgTotalSystemCost, AvgSystemImpact, numberofIncludedFields `
             | ConvertTo-Html -CSSUri ../mi_style.css -Title "Missing indexes Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\$Srvr\"SQLDoc_Missing_Index.htm"
            }
        }
            SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Report [Missing Indexe] created!'
 }
 
 function SQLDoc-Report-JobInfo
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [String]
        $ReportFolder,
 [Parameter(Mandatory = $false)]
        [Switch]
        $CreateSubReports
)

 $InfoQry="select * from JobInfo"

    if (!$PSBoundParameters.ContainsKey('CreateSubReports'))
        {
            $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
            $ReportHeader ="<div class='header'><h1></h1></div>"
            $ReportFooter = "<script src=mi_script.js></script>"
    
            $ReportData | Select ServerName,agentsvc_status,agentsvc_startupType,JobName,Is_enabled,Schedule_Name,Schedule_enabled,IsRunning,RequestSource,LastRunTime,NextRunTime,LastJobStep,RetryAttempt,JobLastOutcome,JobLastDuration,TotalRunsLast7Days,FailedRunsLast7Days,AverageDuration,MaxDuration,Collection_Time `
             | ConvertTo-Html -CSSUri mi_style.css -Title "Jobs Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\"SQLDoc-Jobs-Report.htm"

        }
    else
        {
            $LoopData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "select distinct ServerName from [dbo].[JobInfo]"
            foreach($row in $LoopData)
            {
                $srvr =$row.Item('ServerName')
                $InfoQry="select * from [dbo].[JobInfo] where [ServerName]='$srvr' "

                if(!(Test-Path -path $ReportFolder\$srvr))  
                    {  
                        New-Item -ItemType directory -Path $ReportFolder\$srvr
                    }

                $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
                $ReportHeader ="<div class='header'><h1></h1></div>"
                $ReportFooter = "<script src=../mi_script.js></script>"
    
                $ReportData | Select ServerName,agentsvc_status,agentsvc_startupType,JobName,Is_enabled,Schedule_Name,Schedule_enabled,IsRunning,RequestSource,LastRunTime,NextRunTime,LastJobStep,RetryAttempt,JobLastOutcome,JobLastDuration,TotalRunsLast7Days,FailedRunsLast7Days,AverageDuration,MaxDuration,Collection_Time `
             | ConvertTo-Html -CSSUri ../mi_style.css -Title "Jobs Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\$Srvr\"SQLDoc-Jobs-Report.htm"
            }
        }
        SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Report [Jobs] created!'
 }

function SQLDoc-Report-BackupInfo
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [String]
        $ReportFolder,
 [Parameter(Mandatory = $false)]
        [Switch]
        $CreateSubReports
)

 $InfoQry="select * from BackupInfo"

    if (!$PSBoundParameters.ContainsKey('CreateSubReports'))
        {
            $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
            $ReportHeader ="<div class='header'><h1></h1></div>"
            $ReportFooter = "<script src=mi_script.js></script>"
    
            $ReportData | Select  ServerName, DBname, recovery_model, logSize_mb, rowSize_mb, totalSize_mb, FullBck_count, LogBck_count, OtherBck_count, LastFullBackup, LastLogBackup, MaxFullDuration, MaxLogDuration, MaxBackupSize_mb, Collection_Date `
            | ConvertTo-Html -CSSUri mi_style.css -Title "backup Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\"SQLDoc-Backups-Report.htm"

        }
    else
        {
            $LoopData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "select distinct ServerName from [dbo].[BackupInfo]"
            foreach($row in $LoopData)
            {
                $srvr =$row.Item('ServerName')
                $InfoQry="select * from [dbo].[BackupInfo] where [ServerName]='$srvr' "

                if(!(Test-Path -path $ReportFolder\$srvr))  
                    {  
                        New-Item -ItemType directory -Path $ReportFolder\$srvr
                    }

                $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
                $ReportHeader ="<div class='header'><h1></h1></div>"
                $ReportFooter = "<script src=../mi_script.js></script>"
    
                $ReportData | Select  ServerName, DBname, recovery_model, logSize_mb, rowSize_mb, totalSize_mb, FullBck_count, LogBck_count, OtherBck_count, LastFullBackup, LastLogBackup, MaxFullDuration, MaxLogDuration, MaxBackupSize_mb, Collection_Date `
             | ConvertTo-Html -CSSUri ../mi_style.css -Title "Backups Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\$Srvr\"SQLDoc-Backup-Report.htm"
            }
        }
            SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Report [backups] created!'
 }

 function SQLDoc-Report-AssessmentData
{
param(
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseServer,
 [Parameter(Mandatory = $true)]
        [String]
        $DataWarehouseDatabase,
 [Parameter(Mandatory = $false)]
        [String]
        $ReportFolder,
 [Parameter(Mandatory = $false)]
        [Switch]
        $CreateSubReports
)

 $InfoQry="select * from AssessmentData"

    if (!$PSBoundParameters.ContainsKey('CreateSubReports'))
        {
            $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
            $ReportHeader ="<div class='header'><h1></h1></div>"
            $ReportFooter = "<script src=mi_script.js></script>"
    
            $ReportData | Select   TargetType, Target_Server, Target_Database,Severity ,CheckId, CheckName, Message, RulesetName  ,RulesetVersion,HelpLink `
            | ConvertTo-Html -CSSUri mi_style.css -Title "Assessment Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\"SQLDoc-AssessmentData-Report.htm"

        }
    else
        {
            $LoopData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query "select distinct ServerName from [dbo].[AssessmentData]"
            foreach($row in $LoopData)
            {
                $srvr =$row.Item('ServerName')
                $InfoQry="select * from [dbo].[AssessmentData] where [ServerName]='$srvr' "

                if(!(Test-Path -path $ReportFolder\$srvr))  
                    {  
                        New-Item -ItemType directory -Path $ReportFolder\$srvr
                    }

                $ReportData=Invoke-SqlCmd -ServerInstance $DataWarehouseServer -Database $DataWarehouseDatabase -Query $InfoQry
                $ReportHeader ="<div class='header'><h1></h1></div>"
                $ReportFooter = "<script src=../mi_script.js></script>"
    
            $ReportData | Select   TargetType, Target_Server, Target_Database,Severity ,CheckId, CheckName, Message, RulesetName  ,RulesetVersion,HelpLink `
             | ConvertTo-Html -CSSUri ../mi_style.css -Title "Assessment Report"  -PreContent "$($ReportHeader)" -PostContent "$($ReportFooter)" | Out-File -Encoding utf8 $ReportFolder\$Srvr\"SQLDoc-AssessmentData.htm"
            }
        }
            SQLDoc-Common-MakeLogEntry -DataWarehouseServer $DataWarehouseServer -DataWarehouseDatabase $DataWarehouseDatabase -Message 'Report [Assessment] created!'
 }
 <#

 Export-ModuleMember -Function SQLDoc-Main-CollectGeneralInfo
 Export-ModuleMember -Function SQLDoc-Common-CheckCollectionDB
 Export-ModuleMember -Function SQLDoc-Common-MakeLogEntry
 Export-ModuleMember -Function SQLDoc-MissingIndexes-Collect
 Export-ModuleMember -Function SQLDoc-MissingIndexes-CollectAdditionalInfo
 Export-ModuleMember -Function SQLDoc-MissingIndexes-ValidateIndex
 Export-ModuleMember -Function SQLDoc-MissingIndexes-Create
 Export-ModuleMember -Function SQLDoc-Jobs-Collect
 Export-ModuleMember -Function SQLDoc-PerfCounters-Collect
 Export-ModuleMember -Function SQLDoc-Backups-Collect
 Export-ModuleMember -Function SQLDoc-BadLogins-Collect
 Export-ModuleMember -Function SQLDoc-Assessment-Collect
 Export-ModuleMember -Function  SQLDoc-Report-All
 Export-ModuleMember -Function  SQLDoc-Report-IndexPage
 Export-ModuleMember -Function  SQLDoc-Report-GeneralInfo
 Export-ModuleMember -Function  SQLDoc-Report-MissingIndexes
 Export-ModuleMember -Function  SQLDoc-Report-JobInfo
 Export-ModuleMember -Function  SQLDoc-Report-BackupInfo
 Export-ModuleMember -Function  SQLDoc-Report-AssessmentData


 Export-ModuleMember -Function  SQLDoc-Collect-All
 Export-ModuleMember -Function  SQLDoc-PerfCounters-Collect



 

#SQLDoc-Report-GeneralInfo -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse -ReportFolder "C:\Users\petartr\Desktop\POwershell Module PetarT\Report"  -CreateSubReports
#SQLDoc-Common-CheckCollectionDB -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse
#SQLDoc-Main-CollectGeneralInfo -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse


#SQLDoc-Jobs-Collect -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse
#SQLDoc-Backups-Collect -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse
#SQLDoc-Report-All -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse  -ReportFolder 'C:\Users\petartr\Desktop\POwershell Module PetarT\TestReport'

#SQLDoc-BadLogins-Collect -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse -PasswordListPath 'C:\tempExport\pwdList.txt'

#>



#SQLDoc-Main-CollectGeneralInfo -DataWarehouseServer "(local)"


#SQLDoc-MissingIndexes-Collect -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse


SQLDoc-Collect-All -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse

#SQLDoc-PerfCounters-Collect -DataWarehouseServer Petar_T -DataWarehouseDatabase SQL_Datawarehouse