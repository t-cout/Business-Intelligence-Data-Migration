--**************************************************************************--
-- Title: DWStudentEnrollments ETL Process
-- Desc: This file performs ETL processing for the DWStudentEnrollments database. 
-- Change Log: When,Who,What
-- 2020-01-01,RRoot,Created starter code
--**************************************************************************--

USE DWStudentEnrollments;
Go
Set NoCount On;
Go

--********************************************************************--
-- 0) Create ETL metadata objects
--********************************************************************--
If NOT Exists(Select * From Sys.tables where Name = 'EtlLog')
  Create -- Drop
  Table EtlLog
  (EtlLogID int identity Primary Key
  ,ETLDateAndTime datetime Default GetDate()
  ,ETLAction varchar(100)
  ,EtlLogMessage varchar(2000)
  );
go

Create or Alter View vEtlLog
As
  Select
   EtlLogID
  ,ETLDate = Format(ETLDateAndTime, 'D', 'en-us')
  ,ETLTime = Format(Cast(ETLDateAndTime as datetime2), 'HH:mm:ss', 'en-us')
  ,ETLAction
  ,EtlLogMessage
  From EtlLog;
go


Create or Alter Proc pInsEtlLog
 (@ETLAction varchar(100), @EtlLogMessage varchar(2000))
--*************************************************************************--
-- Desc:This Sproc creates an admin table for logging ETL metadata. 
-- Change Log: When,Who,What
-- 2020-01-01,RRoot,Created Sproc
--*************************************************************************--
As
Begin
  Declare @RC int = 0;
  Begin Try
    Begin Tran;
      Insert Into EtlLog
       (ETLAction,EtlLogMessage)
      Values
       (@ETLAction,@EtlLogMessage)
    Commit Tran;
    Set @RC = 1;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Set @RC = -1;
  End Catch
  Return @RC;
End
Go
-- Truncate Table ETLLog;
-- Exec pInsETLLog @ETLAction = 'Begin ETL',@ETLLogMessage = 'Start of ETL process' 
-- Select * From vEtlLog

--********************************************************************--
-- Pre-load tasksit ne
--********************************************************************--

Go
Create Or Alter Proc pETLDropFks
--*************************************************************************--
-- Desc:This sproc drops foreign key constraints
-- Change Log: When,Who,What
-- 2024-12-07,TCoutermarsh,Created Sproc
--*************************************************************************--

As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000)
  Begin Try
    Alter Table FactEnrollments Drop Constraint fkEnrollmentsToDimClasses;
    Alter Table FactEnrollments Drop Constraint fkEnrollmentsToDimStudents;
    Alter Table FactEnrollments Drop Constraint fkEnrollmentsToDimDates;
	Set @RC = 1;
  End Try
  Begin Catch
	Declare @ErrorMessage nvarchar(1000) = Error_Message();
	Exec pInsEtlLog
		  @ETLAction = 'pETLDropFks'
		 ,@EtlLogMessage = @ErrorMessage;
	Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go

Go
Create Or Alter Proc pETLTruncateTables
--*************************************************************************--
-- Desc:This sproc truncates all tables
-- Change Log: When,Who,What
-- 2024-07-12,TCoutermarsh,Created Sproc
--*************************************************************************--

As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000)
  Begin Try
    Truncate Table FactEnrollments;
	Truncate Table DimStudents;		
	Truncate Table DimClasses;
	Truncate Table DimDates;
	Set @RC = 1;
  End Try
  Begin Catch
	Declare @ErrorMessage nvarchar(1000) = Error_Message();
	Exec pInsEtlLog
		  @ETLAction = 'pETLTruncateTables'
		 ,@EtlLogMessage = @ErrorMessage;
	Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go

--Exec pEtlDropFks; Select * From vEtlLog;
-- Exec pEtlTruncateTables; Select * From vEtlLog;

--********************************************************************--
-- Load dimension tables
--********************************************************************--
Go
Create or Alter Proc pEtlDimDates
--*************************************************************************--
-- Desc:This sproc generates date data for the DimDates tables
-- Change Log: When,Who,What
-- 2020-01-01,RRoot,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 1;
  Declare @Message varchar(1000) 
  Set NoCount On; -- This will remove the 1 row affected msg in the While loop;
  Begin Try
 	  -- Create variables to hold the start and end date
	  Declare @StartDate datetime = '01/01/2015';
	  Declare @EndDate datetime = '12/31/2025'; 
	  Declare @DateInProcess datetime;
      Declare @TotalRows int = 0;

	  -- Use a while loop to add dates to the table
	  Set @DateInProcess = @StartDate;

	  While @DateInProcess <= @EndDate
	    Begin
	      -- Add a row into the date dimensiOn table for this date
	     Begin Tran;
	       Insert Into DimDates 
	       ( [DateKey], [FullDate], [FullDateName], [MonthID], [MonthName], [QuarterID], [QuarterName], [YearID], [YearName] )
	       Values ( 
	   	     Cast(Convert(nvarchar(50), @DateInProcess , 112) as int) -- [DateKey]
	        ,@DateInProcess -- [FullDate]
	        ,DateName( weekday, @DateInProcess ) + ', ' + Convert(nvarchar(50), @DateInProcess , 110) -- [USADateName]  
	        ,Left(Cast(Convert(nvarchar(50), @DateInProcess , 112) as int), 6) -- [MonthKey]   
	        ,DateName( MONTH, @DateInProcess ) + ', ' + Cast( Year(@DateInProcess ) as nVarchar(50) ) -- [MonthName]
	        , Cast(Cast(YEAR(@DateInProcess) as nvarchar(50))  + '0' + DateName( QUARTER,  @DateInProcess) as int) -- [QuarterKey]
	        ,'Q' + DateName( QUARTER, @DateInProcess ) + ', ' + Cast( Year(@DateInProcess) as nVarchar(50) ) -- [QuarterName] 
	        ,Year( @DateInProcess ) -- [YearKey]
	        ,Cast( Year(@DateInProcess ) as nVarchar(50) ) -- [YearName] 
	        ); 
	       -- Add a day and loop again
	       Set @DateInProcess = DateAdd(d, 1, @DateInProcess);
	     Commit Tran;
      Set @TotalRows += 1;
	  End -- While
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsEtlLog
	        @ETLAction = 'pEtlDimDates'
	       ,@EtlLogMessage = @ErrorMessage;
    Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go
-- Exec pEtlDimDates; Select * From DimDates;Select * From vEtlLog;

Go
Create or Alter View vETLDimClasses
--*************************************************************************--
-- Desc:Creates ETL DimClasses view
-- Change Log: When,Who,What
-- 2024-12-07,TCoutermarsh,Created View
--*************************************************************************--
As
Select c.* From OpenRowSet('SQLNCLI11'
,'Server=bidd.database.windows.net;uid=biddadmin;pwd=biddP@$$word;database=StudentEnrollments;' 
, 
'Select 
 ClassID = c.[Id]
,ClassName = Cast(c.[Name] as nvarchar(200))
,ClassStartDate = Cast(c.[StartDate] as Date)
,ClassEndDate = Cast(c.[EndDate] as Date)
,CurrentClassPrice = c.[Price]
,MaxCourseEnrollment = c.[MaxSize]
,ClassroomId = cl.[Id]
,ClassroomName = Cast(cl.[Name] as nvarchar(200))
,ClassroomMaxSize = cl.[MaxSize]
,DepartmentId = d.[Id]
,DepartmentName = Cast(d.[Name] as nvarchar(200))
From [dbo].[Classes] as c
Join [dbo].[Classrooms] as cl
  On c.ClassroomID = cl.Id
Join [dbo].[Departments] as d
  On c.DepartmentId = d.Id
'
) As c;
Go

--Sproc to load data into DimClasses from vETLDimClasses
Create Or Alter Proc pETLDimClasses
--*************************************************************************--
-- Desc:This sproc loads transformed data into DimClasses from vETLDimClasses
-- Change Log: When,Who,What
-- 2024-12-07,TCoutermarsh,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000)
  Begin Try
	Begin Tran
		Insert Into DimClasses
		( [ClassID]
		, [ClassName]
		, [ClassStartDate]
		, [ClassEndDate]
		, [CurrentClassPrice]
		, [MaxCourseEnrollment]
		, [DepartmentID]
		, [DepartmentName]
		, [ClassroomID]
		, [ClassroomName]
		, [ClassroomMaxSize]
		) Select
		  [ClassID]
		, [ClassName]
		, [ClassStartDate]
		, [ClassEndDate]
		, [CurrentClassPrice]
		, [MaxCourseEnrollment]
		, [DepartmentID]
		, [DepartmentName]
		, [ClassroomID]
		, [ClassroomName]
		, [ClassroomMaxSize]
		From vETLDimClasses;
	Commit Tran;
  End Try
  Begin Catch
   If @@TRANCOUNT > 0 Rollback Tran;
	Declare @ErrorMessage nvarchar(1000) = Error_Message();
	Exec pInsEtlLog
		  @ETLAction = 'pETLDimClasses'
		 ,@EtlLogMessage = @ErrorMessage;
	Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go

--Creates view to bring in data from cloud database and make transformations
Go
Create or Alter View vETLDimStudents
--*************************************************************************--
-- Desc:Creates ETL DimStudents view
-- Change Log: When,Who,What
-- 2024-12-07,TCoutermarsh,Created View
--*************************************************************************--
As
SELECT s.* FROM OPENROWSET('SQLNCLI11'
,'Server=bidd.database.windows.net;uid=biddadmin;pwd=biddP@$$word;database=StudentEnrollments;' 
, 
'Select 
 [StudentID] = s.[ID]
,[StudentName] = Cast((s.FirstName +  '' '' + s.LastName) as nvarchar(200))
From [dbo].[Students] as s
'
) AS s;
Go

Create Or Alter Proc pETLDimStudents
--*************************************************************************--
-- Desc:This sproc loads transformed data into DimStudents from vETLDimStudents
-- Change Log: When,Who,What
-- 2024-12-07,TCoutermarsh,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000)
  Begin Try
	Begin Tran
		Insert Into DimStudents
		( [StudentID]
		, [StudentName]
		) Select
		  [StudentID]
		, [StudentName]
		From vETLDimStudents
	Commit Tran;
  End Try
  Begin Catch
   If @@TRANCOUNT > 0 Rollback Tran;
	Declare @ErrorMessage nvarchar(1000) = Error_Message();
	Exec pInsEtlLog
		  @ETLAction = 'pETLDimStudents'
		 ,@EtlLogMessage = @ErrorMessage;
	Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go

--********************************************************************--
-- Load Fact Tables
--********************************************************************--
Go
Create or Alter View vFactEnrollments
--*************************************************************************--
-- Desc:Creates ETL FactEnrollments view
-- Change Log: When,Who,What
-- 2024-12-07,TCoutermarsh,Created View
--*************************************************************************--
As
SELECT 
  [EnrollmentId] = fe.[EnrollmentId]
, [EnrollmentDateKey] = dd.[DateKey]
, [StudentKey] = ds.[StudentKey]
, [ClassKey] = dc.[ClassKey]
, [EnrollmentPrice] = fe.[EnrollmentPrice]--, '---------' as AllColumns, fe.*, ds.*, dc.*
	FROM OPENROWSET('SQLNCLI11'
		,'Server=bidd.database.windows.net;uid=biddadmin;pwd=biddP@$$word;database=StudentEnrollments;' 
		,'Select
		 [EnrollmentId] = [Id]
		,[Date]
		,[StudentId]
		,[ClassId]
		,[EnrollmentPrice] = [Price]
		From [dbo].[Enrollments]'
		) AS fe
	JOIN [dbo].[DimStudents] as ds
	  On fe.[StudentId] = ds.StudentID
	JOIN [dbo].[DimClasses] as dc
	  On fe.[ClassId] = dc.ClassID
	JOIN [dbo].[DimDates] as dd
	  On Cast(fe.Date as date) = dd.FullDate
	  ;
Go



Create Or Alter Proc pETLFactEnrollments
--*************************************************************************--
-- Desc:This sproc loads transformed data into FactEnrollments from vFactEnrollments
-- Change Log: When,Who,What
-- 2024-12-07,TCoutermarsh,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000)
  Begin Try
	Begin Tran;
	Insert Into [dbo].[FactEnrollments]
	([EnrollmentId], [DateKey], [StudentKey], [ClassKey], [EnrollmentPrice])
	Select
	[EnrollmentId], [EnrollmentDateKey], [StudentKey], [ClassKey], [EnrollmentPrice]
	From vFactEnrollments
	Commit Tran;
  End Try
  Begin Catch
   If @@TRANCOUNT > 0 Rollback Tran;
	Declare @ErrorMessage nvarchar(1000) = Error_Message();
	Exec pInsEtlLog
		  @ETLAction = 'pETLFactEnrollments'
		 ,@EtlLogMessage = @ErrorMessage;
	Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go


--********************************************************************--
-- Post-load Tasks
--********************************************************************--
Go
Create Or Alter Proc pEtlReplaceFKs
--*************************************************************************--
-- Desc:This sproc replaces foreign key constraints
-- Change Log: When,Who,What
-- 2024-12-07,TCoutermarsh,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000)
  Begin Try
Alter Table FactEnrollments
  Add Constraint fkEnrollmentsToDimClasses
  Foreign Key (ClassKey) References DimClasses(ClassKey);

Alter Table FactEnrollments
  Add Constraint fkEnrollmentsToDimStudents
  Foreign Key (StudentKey) References DimStudents(StudentKey);

Alter Table FactEnrollments
  Add Constraint fkEnrollmentsToDimDates
  Foreign Key (DateKey) References DimDates(DateKey);

  Set @RC = 1;
  End Try
  Begin Catch
	Declare @ErrorMessage nvarchar(1000) = Error_Message();
	Exec pInsEtlLog
		  @ETLAction = 'pETLReplaceFks'
		 ,@EtlLogMessage = @ErrorMessage;
	Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go

--********************************************************************--
-- Review the results of this script
--********************************************************************--
Go
Exec pInsETLLog @ETLAction = 'Begin ETL', @ETLLogMessage = 'Start of ETL process' 
Exec pEtlDropFks; 
Exec pEtlTruncateTables;
Exec pEtlDimDates; Select top 10 * From vDimDates;
Exec pEtlDimClasses; Select * From vDimClasses;
Exec pEtlDimStudents; Select * From vDimStudents;
Exec pEtlFactEnrollments; Select * From vFactEnrollments;
Exec pEtlReplaceFKs;
Exec pInsETLLog @ETLAction = 'End ETL', @ETLLogMessage = 'End of ETL process' 
Select * From vEtlLog
