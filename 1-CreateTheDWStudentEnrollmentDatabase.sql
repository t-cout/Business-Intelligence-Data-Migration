--**************************************************************************--
-- Title: Create the DWStudentEnrollments database
-- Desc: This file will drop and create the DWStudentEnrollments database. 
-- Change Log: When,Who,What
-- 2024-12-03,TCoutermarsh,Created starter code
--**************************************************************************--
Set NoCount On;

USE [master]
If Exists (Select Name from SysDatabases Where Name = 'DWStudentEnrollments')
  Begin
   ALTER DATABASE DWStudentEnrollments SET SINGLE_USER WITH ROLLBACK IMMEDIATE
   DROP DATABASE DWStudentEnrollments
  End
Go
Create Database DWStudentEnrollments;
Go
USE DWStudentEnrollments;


--********************************************************************--
-- Create the Tables
--********************************************************************--
Create Table DimDates
([DateKey] int Not Null Constraint pkDimDates Primary Key
,[FullDate] date Not Null
,[FullDateName] nvarchar(100) Not Null
,[MonthID] int Not Null
,[MonthName] nvarchar(100) Not Null
,[QuarterID] int Not Null
,[QuarterName] nvarchar(100) Not Null
,[YearID] int Not Null
,[YearName] nvarchar(100) Not Null
);
Go

Create Table DimClasses
([ClassKey] int Identity Constraint pkDimClasses Primary Key
,[ClassID] int Not Null
,[ClassName] nvarchar(200) Not Null	
,[ClassStartDate] date Not Null	
,[ClassEndDate] date Not Null
,[CurrentClassPrice] money Not Null
,[MaxCourseEnrollment] int Not Null
,[DepartmentID] int Not Null
,[DepartmentName] nvarchar(200) Not Null
,[ClassroomID] int Not Null 
,[ClassroomName] nvarchar(200) Not Null	
,[ClassroomMaxSize] int Not Null	
);
go

Create Table DimStudents
([StudentKey] int Identity Constraint pkDimStudents Primary Key
,[StudentID] int Not Null
,[StudentName] nvarchar(200) Not Null	
);
go

Create Table FactEnrollments
([EnrollmentID] int Not Null
,[DateKey] int Not Null
,[StudentKey] int Not Null
,[ClassKey] int Not Null
,[EnrollmentPrice] money Not Null
Constraint pkFactEnrollments Primary Key ([EnrollmentID],[DateKey],[StudentKey],[ClassKey])
);
go
--********************************************************************--
-- Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--
Alter Table FactEnrollments
  Add Constraint fkEnrollmentsToDimClasses
  Foreign Key (ClassKey) References DimClasses(ClassKey);
go

Alter Table FactEnrollments
  Add Constraint fkEnrollmentsToDimStudents
  Foreign Key (StudentKey) References DimStudents(StudentKey);
go

Alter Table FactEnrollments
  Add Constraint fkEnrollmentsToDimDates
  Foreign Key (DateKey) References DimDates(DateKey);
go

--********************************************************************--
-- Create the Abstraction Layers
--********************************************************************--
Create View vDimDates
As
Select *
From DimDates;
go

Create View vDimClasses
As
Select *
From DimClasses;
go

Create View vDimStudents
As
Select *
From DimStudents;
go

Create View vFactEnrollments
As
Select *
From FactEnrollments;
go

-- Base Views

-- Metadata View
Go
Create or Alter View vMetaDataStudentEnrollments
As
Select Top 100 Percent
 [Source Table] = DB_Name() + '.' + SCHEMA_NAME(tab.[schema_id]) + '.' + object_name(tab.[object_id])
,[Source Column] =  col.[Name]
,[Source Type] = Case 
				When t.[Name] in ('char', 'nchar', 'varchar', 'nvarchar' ) 
				  Then t.[Name] + ' (' +  format(col.max_length, '####') + ')'                
				When t.[Name]  in ('decimal', 'money') 
				  Then t.[Name] + ' (' +  format(col.[precision], '#') + ',' + format(col.scale, '#') + ')'
				 Else t.[Name] 
                End 
,[Source Nullability] = iif(col.is_nullable = 1, 'Null', 'Not Null') 
From Sys.Types as t 
Join Sys.Columns as col 
 On t.system_type_id = col.system_type_id 
Join Sys.Tables tab
  On tab.[object_id] = col.[object_id]
And t.name <> 'sysname'
Order By [Source Table], col.column_id; 
go
Select * From vMetaDataStudentEnrollments;
