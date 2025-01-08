--**************************************************************************--
-- Title: DWStudentEnrollments ETL Views for Tabular Model
-- Desc: This file creates DWStudentEnrollments ETL Views for Tabular Model. 
-- Change Log: When,Who,What
-- 2024-12-09,TCoutermarsh,Created starter code
--**************************************************************************--
USE DWStudentEnrollments;
Go
Set NoCount On;
Go

Create or Alter View vTabularETLDimDates
As
Select
 [DateKey] = Convert(date, Cast([DateKey] as char(8)), 110)
,[WeekdayandDate] = [FullDateName]
,[Month] = [MonthName]
,[Quarter] = [QuarterName]
,[Year] = [YearName]
From [DWStudentEnrollments].[dbo].[DimDates]
Go

Create or Alter View vTabularETLDimClasses
As
Select
 [ClassKey]
,[ClassID]
,[ClassName]
,[ClassStartDate]
,[ClassEndDate]
,[CurrentClassPrice]
,[MaxCourseEnrollment]
,[DepartmentID]
,[DepartmentName]
,[ClassroomID]
,[ClassroomName]
,[ClassroomMaxSize]
From [DWStudentEnrollments].[dbo].[DimClasses]
Go

Create or Alter View vTabularETLDimStudents
As
Select
 [StudentKey]
,[StudentID]
,[StudentName]
From [DWStudentEnrollments].[dbo].[DimStudents]
Go

Create or Alter View vTabularETLFactEnrollments
As
Select
 [EnrollmentID]
,[DateKey] = Convert(date, Cast([DateKey] as char(8)), 110)
,[StudentKey]
,[ClassKey]
,[EnrollmentPrice]
From [DWStudentEnrollments].[dbo].[FactEnrollments]
Go

Select * From vTabularETLDimDates;
Select * From vTabularETLDimClasses;
Select * From vTabularETLDimStudents;
Select * From vTabularETLFactEnrollments;
