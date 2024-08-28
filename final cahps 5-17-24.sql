
;with cte_member_demographics as
(
SELECT 
      [MemberNumber] as MemberId
      ,dbo.Capitalize([MemberFirstName])  as FirstName
      ,dbo.Capitalize([MemberLastName]) as LastName
      ,try_cast(EnrollmentStartDate as date) as EnrollmentStartDate
      ,try_cast(EnrollmentEndDate as date) as EnrollmentEndDate
      ,[CmsCONTRACTId] as Contract
      ,[PBP] as planid
	  ,[MemberDOB] as DOB
	  ,[MemberState] as MemberState
      ,[MemberADDRESS] as  MemberAddress
      ,[MemberCity] as City
	  ,[MemberCounty] as County
	  ,[MemberGender] as Gender
	  ,[ProviderNumber] as ProviderId
	  ,[RiskScore] as RiskScore
	  ,[Language] as Language
      ,[MemberZipCode] as ZipCode  
	  ,DATEDIFF(YEAR, MemberDOB, GETDATE()) AS age
      ,[ethnicity] as Ethnicity
	  ,[Race] as Race
      ,ROW_NUMBER()over(partition by [MemberNumber] order by [EnrollmentEndDate] DESC) as rn
	   
  FROM [dbo].[INGAGE_MEMBER_BASE]
  	) 
select
	[MemberId],
	[FirstName],
	[LastName],
	[DOB],
	[Age],
	[Gender],
    NULL as [PhoneNumber],
    [City],
    [County],
    [ZipCode],
    [MemberState],
    [planId],
    [MemberAddress],
    [Contract],
    [RiskScore],
    [ProviderId],
    [Race],
    [Ethnicity],
    [EnrollmentStartDate],
    [EnrollmentEndDate],
    [Language]
into #cte_member_demographics_final
	from cte_member_demographics where rn =1; ------(73610 rows affected)

	select top 10 * from INGAGE_MEMBER_BASE
--drop table #temp_Mock_cahpsOverview
SELECT [MemberId]
	  ,[Code]
      ,[QuestionId]
      , Try_cast([QuestionAnsValue] as float) as [QuestionScore]
into #temp_Mock_cahpsOverview
  FROM [dbo].[MemberCahpsOverview] ------------ (30311 rows affected)


 /* 
select distinct MemberId into #uniqueMemberCahpsOverview_2022 from MemberCAHPSOverview_History
where Year=2022

*/
--drop table #temp_mockcahps_measure_score

select distinct MemberId into  #uniqueMemberCahpsOverview from #temp_Mock_cahpsOverview ----(2266 rows affected)


select distinct MemberId,Code into #measure_uniqueMemberCahpsOverview from #temp_Mock_cahpsOverview------(16110 rows affected)

select MemberId,Code,avg(QuestionScore) as MeasureScore
into #temp_mockcahps_measure_score
from #temp_Mock_cahpsOverview
group by MemberId,Code
select * from #temp_mockcahps_measure_score ---------(16110 rows affected)


select MemberId,avg(MeasureScore) as MemberMockScore
into #temp_mockcahps_member_score from #temp_mockcahps_measure_score
group by MemberId ------------(2266 rows affected)


-- Cahps score
----------------------

-- Measure Score
-------------------
declare @last_year int=0  
set @last_year=(select max(Year(CreatedOn))
  FROM Chaps_Prediction_MemberCahpsOverview_History)-------2024
print(@last_year)


declare @last_month int=0  
set @last_month=(select max(Month)
  FROM Chaps_Prediction_MemberCahpsOverview_History ---------5
   where Year(CreatedOn)=@last_year) 
print(@last_month)



/*
declare @last_month int=0  
set @last_month=(select max(Month)
  FROM Chaps_Prediction_MemberCahpsOverview_History 
   where Year(CreatedOn)=@last_year) 
*/



--drop table #Latest_Prediction_MemberCahpsOverview
select * into #Latest_Prediction_MemberCahpsOverview
from Chaps_Prediction_MemberCahpsOverview_History
where Month = @last_month and Year(CreatedOn) = @last_year
select * from #Latest_Prediction_MemberCahpsOverview

--drop table #Previous_Prediction_MemberCahpsOverview
select * into #Previous_Prediction_MemberCahpsOverview
from Chaps_Prediction_MemberCahpsOverview_History
where Month=
case when @last_month=1 then 12
else @last_month-1 end and Year(CreatedOn) = @last_year

select * from #Previous_Prediction_MemberCahpsOverview

-- mbi not exist in cahps
select  MemberId,Status, Month,70.7 as MemberScore into #mbi_does_not_exists_pred_chaps_member_status   from (
select distinct a.MemberId,'Impactable' as Status ,@last_month as Month
from #cte_member_demographics_final a left join
#Latest_Prediction_MemberCahpsOverview b on   a.MemberId=b.MemberId
where b.MemberId is null

union all    

select distinct a.MemberId,'impactable' as Status,case when @last_month=1 then 12 else @last_month-1 end as Month
from #cte_member_demographics_final a left join
#previous_prediction_membercahpsoverview b on  a.MemberId=b.MemberId
where b.MemberId is null

) a--------------(147214 rows affected)

select * from QuestionMap

;with cte as(
	select Code,QuestionCode from QuestionMap 
	where QuestionCode in ('RDP','GNRx1','GNRx2','GNRx3') -- need to update when new measure will add
	
	union all
	
	select 'D06' as Code,'GNRx2_3' as QuestionCode
	
	--union all
	
	--select 'D06' as Code,'q42q44' as QuestionCode
	
)select MemberId,Code,QuestionCode as QuestionId,Status,MemberScore,Month into #mbi_does_not_exists_pred_chaps_question_status_pre 
from #mbi_does_not_exists_pred_chaps_member_status,cte; ------------(736070 rows affected)



select MemberId,Code, QuestionId,Status ,
case when QuestionId='RDP'  then 61.76
when QuestionId='GNRx1'  then 59.62
when QuestionId='GNRx2'  then 59.23
when QuestionId='GNRx3'  then 62.71
when QuestionId='GNRx2_3'  then 61.93

/*
when QuestionId='q04'  then 59.02
when QuestionId='q06'  then 58.63
when QuestionId='q18'  then 62.95
when QuestionId='q23'  then 57.1
when QuestionId='q26'  then 50
when QuestionId='q32'  then 100
when QuestionId='q45'  then 75.9 
when QuestionId='q20q21'  then 61.77

when QuestionId='q09'  then 96.36 
when QuestionId='q40'  then 100 
when QuestionId='q42q44'  then 100
*/
 -- need to update when new measure will add
end as QustionScore,Month
into #mbi_does_not_exists_pred_chaps_question_status
 from #mbi_does_not_exists_pred_chaps_question_status_pre ----------------(736070 rows affected)



-- combine not match and match item question status 

select MemberId,Code,QuestionId,Status,QustionScore,Month into #Prediction_question_status 
from (
select MemberId,Code,QuestionId,Status,QustionScore,Month
from #Latest_Prediction_MemberCahpsOverview

union all

select MemberId,Code,QuestionId,Status,QustionScore,Month
from #Previous_Prediction_MemberCahpsOverview

union all

select MemberId,Code,QuestionId,Status,QustionScore,Month from #mbi_does_not_exists_pred_chaps_question_status
) a ----------(736088 rows affected)



select MemberId,Code,Status, Month, MeasureScore ,
dbo.CalculateCahpsStarRating(Code,'Measure',MeasureScore) as StarCutPoint
into #mbi_does_not_exists_pred_chaps_measure_status from(
select MemberId,Code,Status,Month,avg(QustionScore) as MeasureScore
from #mbi_does_not_exists_pred_chaps_question_status  
group by MemberId,Code,Status,Month
)a ----------(294428 rows affected)





select MemberId,Code,MeasureScore,Month,
dbo.CalculateCahpsStarRating(Code,'Measure',MeasureScore) as StarCutPoint
into #measure_cahps_code_score
from (

select MemberId,Code,avg(QustionScore) as MeasureScore,Month
from #Latest_Prediction_MemberCahpsOverview
where Code!='C20' and Code!='C21'
group by MemberId,Code,Month

union all

select MemberId,Code,avg(QustionScore) as MeasureScore,Month
from #Previous_Prediction_MemberCahpsOverview
where Code!='C20' and Code!='C21'
group by MemberId,Code,Month


) a -------(12 rows affected)




select MemberId,Code,MeasureScore,Month,StarCutPoint,case when StarCutPoint<=1 then 'Unsatisfied'
	when StarCutPoint>=2 and StarCutPoint<=3 then 'Impactable'
	when StarCutPoint>=4  then 'Satisfied'
	end MeasureStatus
	into #measure_cahps_code_score_status
from
#measure_cahps_code_score------------(12 rows affected)



--select top 100 * from #measure_member_satisfaction_unsatisfaction

select MemberId,Code,Status,MeasureScore,Month,StarCutPoint into #measure_member_satisfaction_unsatisfaction from (
select a.MemberId,a.Code, case when b.MemberId is not null and MeasureStatus='Unsatisfied' then 'Unsatisfied'
when b.MemberId is null and MeasureStatus='Unsatisfied' then 'Inferred Unsatisfied'
when b.MemberId is not null and MeasureStatus='Impactable' then 'Impactable'
when b.MemberId is null and MeasureStatus='Impactable' then 'Inferred Impactable'
when b.MemberId is not null and MeasureStatus='Satisfied' then 'Satisfied'
when b.MemberId is  null and MeasureStatus='Satisfied' then 'Inferred Satisfied'
end as Status,MeasureScore,Month,StarCutPoint
 from #measure_cahps_code_score_status a left join
#measure_uniqueMemberCahpsOverview b on   a.MemberId=b.MemberId
and a.Code=b.Code

union all
select MemberId,Code,Status,QustionScore as MeasureScore,Month,
dbo.CalculateCahpsStarRating(Code,'Measure',QustionScore) as StarCutPoint
from #Latest_Prediction_MemberCahpsOverview
where Code='C20' or Code='C21'

union all
select MemberId,Code,Status,QustionScore as MeasureScore,Month,
dbo.CalculateCahpsStarRating(Code,'Measure',QustionScore) as StarCutPoint
from #Previous_Prediction_MemberCahpsOverview
where Code='C20' or Code='C21'

union all
select MemberId,Code,Status,MeasureScore,Month,StarCutPoint
from #mbi_does_not_exists_pred_chaps_measure_status

)a ------------(294440 rows affected)



----------- baad jabe -------
declare @last_year int=0  
set @last_year=(select max(Year(CreatedOn))
  FROM Chaps_Prediction_MemberCahpsOverview_History)
print(@last_year)


declare @last_month int=0  
set @last_month=(select max(Month)
  FROM Chaps_Prediction_MemberCahpsOverview_History 
   where Year(CreatedOn)=@last_year) 
print(@last_month)


select MemberId,Code,Status,MeasureScore,Month,StarCutPoint
into #latest_measure_member_satisfaction_unsatisfaction  
  from #measure_member_satisfaction_unsatisfaction 
 where Month=@last_year 
 
select MemberId,Code,Status,MeasureScore,Month,StarCutPoint
into #previous_measure_member_satisfaction_unsatisfaction from 
 #measure_member_satisfaction_unsatisfaction
 where Month=case when @last_month=1 then 12
else @last_month-1  end
 
 
 
 select a.MemberId,a.Code,a.Status as MemberCurrentCodeStatus, b.Status as MemberPreviousCodeStatus,
 a.MeasureScore as MemberCurrentCodeScore,b.MeasureScore as MemberPreviousCodeScore ,a.Month,a.StarCutPoint
 into #all_measure_member_satisfaction_unsatisfaction
 from #latest_measure_member_satisfaction_unsatisfaction a
 left join #previous_measure_member_satisfaction_unsatisfaction b 
 on   a.MemberId=b.MemberId and a.Code=b.Code

---------------------
-- Member Score


select distinct  MemberId,Month ,[MemberScore],[MemberStatus],[MemberMovingStatus] 
into #member_prediction_summary
from 
(
select  MemberId,Month ,[MemberScore],[MemberStatus],[MemberMovingStatus] from  #Latest_Prediction_MemberCahpsOverview

union all

select  MemberId,Month ,[MemberScore],[MemberStatus],[MemberMovingStatus] from  #Previous_Prediction_MemberCahpsOverview
) a --------(6 rows affected)



select MemberId,Month,MemberScore,Status,MemberMovingStatus into #member_satisfaction_unsatisfaction from (
select MemberId,MemberScore,MemberStatus as Status,Month,MemberMovingStatus
from #member_prediction_summary 



union all

select MemberId,MemberScore,Status,Month,'Unchange' as MemberMovingStatus from #mbi_does_not_exists_pred_chaps_member_status 

) a------------(147220 rows affected)




declare @last_year int=0  
set @last_year=(select max(Year(CreatedOn))
  FROM Chaps_Prediction_MemberCahpsOverview_History)
print(@last_year)


declare @last_month int=0  
set @last_month=(select max(Month)
  FROM Chaps_Prediction_MemberCahpsOverview_History 
   where Year(CreatedOn)=@last_year) 
print(@last_month)


select distinct MemberId,Month,Status ,MemberScore,MemberMovingStatus
into #latest_member_satisfaction_unsatisfaction from 
 #member_satisfaction_unsatisfaction 
 where Month=@last_month
 
 select distinct MemberId,Month,Status ,MemberScore,MemberMovingStatus
into #previous_member_satisfaction_unsatisfaction from 
 #member_satisfaction_unsatisfaction 
 where Month=case when @last_month=1 then 12
else @last_month-1 end
 
 
 select a.MemberId,a.Month,a.Status as MemberCurrentStatus,b.Status as MemberPreviousStatus,a.MemberScore as MemberCurrentPredictedScore,
 b.MemberScore as MemberPreviousPredictedScore,a.MemberMovingStatus
 into #all_member_cahps_satisfaction_unsatisfaction
 from #latest_member_satisfaction_unsatisfaction a left join 
 #previous_member_satisfaction_unsatisfaction b on 
   a.MemberId=b.MemberId; ---------(73610 rows affected)



   declare @last_year int=0  
set @last_year=(select max(Year(CreatedOn))
  FROM Chaps_Prediction_MemberCahpsOverview_History)
print(@last_year)


declare @last_month int=0  
set @last_month=(select max(Month)
  FROM Chaps_Prediction_MemberCahpsOverview_History 
   where Year(CreatedOn)=@last_year) 
print(@last_month)


select MemberId,
Count(CASE WHEN Status='Satisfied' or Status='Inferred Satisfied' THEN 1 END) as TotalSatisfied,
Count(CASE WHEN Status='Impactable' or Status='Inferred Impactable' THEN 1 END) as TotalImpactable,
Count(CASE WHEN Status='Unsatisfied' or Status='Inferred Unsatisfied' THEN 1 END) as TotalUnsatisfied
into #member_cahps_satisfaction_unsatisfaction from #Prediction_question_status
where Month=@last_month 
group by MemberId 

----------(73610 rows affected)






select a.MemberId,a.TotalSatisfied,a.TotalImpactable,a.TotalUnsatisfied,
b.MemberCurrentStatus,b.MemberCurrentPredictedScore,b.MemberPreviousStatus,b.MemberPreviousPredictedScore,b.MemberMovingStatus
into #member_cahps_info from #member_cahps_satisfaction_unsatisfaction 
a join #all_member_cahps_satisfaction_unsatisfaction b
on   a.MemberId=b.MemberId;-------------(73610 rows affected)

---------

--moving summmary   ((Eikhane Change Asbeeeee ))))-----------
select a.MemberId,a.MemberCurrentStatus,a.MemberMovingStatus, b.Contract,b.PlanId,
b.MemberAddress,b.Ethnicity,b.Race,b.County,b.MemberState ,b.age,
b.DOB,b.PhoneNumber,b.Gender,b.ProviderId,b.RiskScore
into #member_moving_summary_pre
from #all_member_cahps_satisfaction_unsatisfaction a inner join #cte_member_demographics_final b
on   a.MemberId=b.MemberId;----------(73610 rows affected)



drop table #member_moving_summary_pre

select MemberId, Contract,PlanId,

MemberAddress,Ethnicity,Race,County,MemberState ,age,
DOB,PhoneNumber,Gender,ProviderId,RiskScore,
Count(CASE WHEN MemberCurrentStatus='Satisfied' THEN 1 END) as TotalSatisfied,
Count(CASE WHEN MemberCurrentStatus='Impactable' THEN 1 END) as TotalImpactable,
Count(CASE WHEN MemberCurrentStatus='Unsatisfied'  THEN 1 END) as TotalUnsatisfied,
Count(CASE WHEN MemberMovingStatus='SatisfiedToUnsatisfied' THEN 1 END) as TotalSatisfiedToUnsatisfied,
Count(CASE WHEN MemberMovingStatus='SatisfiedToImpactable' THEN 1 END) as TotalSatisfiedToImpactable,
Count(CASE WHEN MemberMovingStatus='ImpactableToUnsatisfied'  THEN 1 END) as TotalImpactableToUnsatisfied,
Count(CASE WHEN MemberMovingStatus='ImpactableToSatisfied' THEN 1 END) as TotalImpactableToSatisfied,
Count(CASE WHEN MemberMovingStatus='UnsatisfiedToImpactable' THEN 1 END) as TotalUnsatisfiedToImpactable,
Count(CASE WHEN MemberMovingStatus='UnsatisfiedToSatisfied'  THEN 1 END) as TotalUnsatisfiedToSatisfied,
Count(CASE WHEN MemberMovingStatus='Unchange'  THEN 1 END) as TotalUnchange
into #member_moving_summary
from #member_moving_summary_pre a 
group by MemberId, Contract,PlanId,
MemberAddress,Ethnicity,Race,County,MemberState ,age,
DOB,PhoneNumber,Gender,ProviderId,RiskScore 
/*Warning: Null value is eliminated by an aggregate or other SET operation.

(73514 rows affected)*/

---score trend

select Month,a.Code,avg(MemberCurrentCodeScore) as MemberCurrentCodeScore, avg(MemberPreviousCodeScore) as MemberPreviousCodeScore
,avg(MeasureScore) as MemberCurrentMockChapsCodeScore
into #temp_cahps_code_score
from #all_measure_member_satisfaction_unsatisfaction a
left join #temp_mockcahps_measure_score b on
    a.MemberId=b.MemberId and
  a.Code=b.Code
group by Month,a.Code ---------(0 rows affected)



truncate table Associate_Cahps_Question_Member_Summary
INSERT INTO [dbo].[Associate_Cahps_Question_Member_Summary]
           ([MemberId],[FirstName],[LastName],[Dob] ,[Gender] ,[PhoneNumber],[City]
           ,[County],[ZipCode],[State],[MemberAddress1]

           ,[Contract]
           ,[PlanId]
		   ,[Age] 
	,[AgeRange] 
	,AttributedMedicalGroup
	,MedGroupLocation
	,[GicCompliance]
	,GicComplianceRange
	,[RiskScore] 
	,[RiskScoreRange]
           ,[StateName]
           ,[TotalGap]
           ,[CloseGap]
           ,[OpenGap]
           ,[Race]
		   ,Ethnicity
	,[EnrollmentStartDate]
,[EnrollmentEndDate]
           ,[Code]
           ,[QuestionId]
           ,[SurveySatisfaction]
           ,[QustionScore]
           ,[HasCahps]
		   ,ProviderId
		   ,[ProviderTaxId]
		   ,[ProviderNpi])
 SELECT a.[MemberId]
      ,a.[FirstName]
      ,a.[LastName]
	  ,a.Dob 
	  ,a.Gender 
      ,a.[PhoneNumber]
      ,a.[City]
      ,a.[County]
      ,a.[ZipCode]
      ,a.[State]
      ,a.[MemberAddress1]

      ,a.[Contract]
      ,a.[PlanId]
	  		   ,[Age] 
	,[AgeRange] 
	,[AttributeMedicalGroup]
	,MedGroupLocation
	,a.[GicCompliance]
	,a.[GicComplianceRange]
	,a.[RiskScore]
	,a.[RiskScoreRange]
      ,a.[StateName]
      ,a.[TotalGap]
      ,a.[CloseGap]
      ,a.[OpenGap]
      ,a.[Race]
		   ,Ethnicity
	,[EnrollmentStartDate]
,[EnrollmentEndDate]
	  ,b.Code
	  ,b.QuestionId
	  ,b.Status
	  ,b.QustionScore
	  ,case when c.MemberId is not null then 1 else 0 end as HasCahps
	  	,ProviderId
	  ,a.ProviderTaxId
	  ,a.ProviderNpi
  FROM [dbo].[Associate_MemberBasic] a
    inner join #Latest_Prediction_MemberCahpsOverview b
  on  a.MemberId=b.MemberId
  left join #temp_Mock_cahpsOverview c on b.MemberId=c.MemberId and  b.QuestionId=c.QuestionId;
  ---------------- (18 rows affected)


truncate table Member_Cahps_Measure_Satisfaction
INSERT INTO [dbo].[Member_Cahps_Measure_Satisfaction]
           ([MemberId]
           ,[Code]
           ,[SurveyStatus]
           ,[Score]
           ,[StartCutPoint]
		   ,[ProviderTaxId]
           ,[ProviderNpi]
		   ,ProviderID )
		     select
       a.[MemberId]
      ,a.[Code]
      ,a.Status
	   ,a.MeasureScore
		    ,a.StarCutPoint
			     ,b.[ProviderTaxId]
			      ,b.[ProviderNpi]
					,b.ProviderId
  FROM #latest_measure_member_satisfaction_unsatisfaction a
  inner join  Associate_MemberBasic b on
   a.MemberId=b.MemberId; ----------- (0 rows affected)







truncate table Chaps_Associate_MemberBasic;
   declare @last_year int=0  
set @last_year=(select max(Year(CreatedOn))
  FROM Chaps_Prediction_MemberCahpsOverview_History)
print(@last_year)
declare @last_month int=0  
set @last_month=(select max(Month)
  FROM Chaps_Prediction_MemberCahpsOverview_History 
   where Year(CreatedOn)=@last_year) 
print(@last_month)
INSERT INTO [dbo].[Chaps_Associate_MemberBasic]
           ([MemberId]
           ,[FirstName]
           ,[LastName]
           ,[PhoneNumber]
		   ,[Age]
		   ,AgeRange
           ,[City]
           ,[County]
           ,[ZipCode]
           ,[State]
           ,[StateName]
           ,[MemberAddress1]

           ,[Contract]
           ,[PlanId]
		   ,[GicCompliance] 
	,[GicComplianceRange]
	,[AttributeMedicalGroup] 
	,[MedGroupLocation] 
           ,[Race]
		   ,[Ethnicity]
		   ,[RiskScore]
		   ,[RiskScoreRange]
           ,[TotalSatisfied]
           ,[TotalImpactable]
           ,[TotalUnsatisfied]
           ,[TotalGap]
           ,[CloseGap]
           ,[OpenGap]
           ,[MemberCurrentStatus]
           ,[MemberPreviousStatus]
           ,[MemberCurrentPredictedScore]
           ,[MemberPreviousPredictedScore]
           ,[HasMockCahps]
           ,[MemberCurrentMockChapsScore]
           ,[MemberMovingStatus]
           ,[MonthName]
           ,[Year]
           ,[CreatedOn]
)
		   
		   select a.[MemberId]
           ,a.[FirstName]
           ,a.[LastName]
           ,a.[PhoneNumber]
		   ,a.[Age]
		   ,a.AgeRange
           ,a.[City]
           ,a.[County]
           ,a.[ZipCode]
           ,a.[State]
           ,a.[StateName]
           ,a.[MemberAddress1]

           ,a.[Contract]
           ,a.[PlanId]
		   ,a.[GicCompliance] 
	,a.[GicComplianceRange]
	,a.[AttributeMedicalGroup] 
	,a.[MedGroupLocation] 

         ,a.[Race]
		 ,a.[Ethnicity]
		 ,a.[RiskScore]
		   ,a.[RiskScoreRange]
		    ,coalesce(c.TotalSatisfied,0) as TotalSatisfied
		   ,coalesce(c.TotalImpactable,0) as TotalImpactable
		   ,coalesce(c.TotalUnsatisfied,0) as TotalUnsatisfied 
		   ,ISNULL(b.[TotalGap],0) as TotalGap
           ,ISNULL(b.[CloseGap],0) as CloseGap
           ,ISNULL(b.[OpenGap],0) as OpenGap
		   ,c.MemberCurrentStatus
		   ,c.MemberPreviousStatus
		   ,c.MemberCurrentPredictedScore
		   ,c.MemberPreviousPredictedScore
		   ,case when d.MemberId is not null then 1 else 0 end as HasMockCahps
		   ,f.MemberMockScore
		   ,c.MemberMovingStatus
           ,@last_month as MonthName
		   ,YEAR(GETDATE()) as Year
		   ,CONVERT(date, GETDATE()) as CreatedOn
		   from
		   	[dbo].[Associate_MemberBasic] a left join 
		[dbo].[Associate_Summary_MemberGap] b on a.MemberId=b.MemberId
		left join #member_cahps_info c on a.MemberId=c.MemberId
		left join #uniqueMemberCahpsOverview d on a.MemberId=d.MemberId
		left join #temp_mockcahps_member_score f on a.MemberId=f.MemberId  ------------85265 
		   

		

truncate table Chaps_Associate_Code_Member_Summary;
INSERT INTO [dbo].[Chaps_Associate_Code_Member_Summary]
           ([MemberId]
           ,[FirstName]
           ,[LastName]
           ,[City]
           ,[County]
           ,[ZipCode]
           ,[State]
           ,[StateName]
           ,[MemberAddress1]

           ,[Contract]
           ,[PlanId]
		    ,[Age]
		   ,AgeRange
           		   ,[GicCompliance] 
	,[GicComplianceRange]
	,[AttributeMedicalGroup] 
	,[MedGroupLocation] 
           ,[Race]
            ,[Ethnicity]
		 ,[RiskScore]
		   ,[RiskScoreRange]
           ,[TotalSatisfied]
           ,[TotalImpactable]
           ,[TotalUnsatisfied]
           ,[TotalGap]
           ,[CloseGap]
           ,[OpenGap]
           ,[Code]
           ,[MemberCurrentCodeStatus]
           ,[MemberPreviousCodeStatus]
           ,[MemberCurrentCodeScore]
           ,[MemberPreviousCodeScore]
           ,[MemberCurrentMockChapsCodeScore]
           ,[MemberCurrentStatus]
           ,[MemberPreviousStatus]
           ,[MemberCurrentPredictedScore]
           ,[MemberPreviousPredictedScore]
           ,[HasMockCahps]
           ,[MemberCurrentMockChapsScore]
           ,[MemberMovingStatus]
           ,[MonthName]
           ,[Year]
           ,[CreatedOn]
)
		   select
		   a.[MemberId]
      ,a.[FirstName]
      ,a.[LastName]
      ,a.[City]
      ,a.[County]
      ,a.[ZipCode]
      ,a.[State]
      ,a.[StateName]
      ,a.[MemberAddress1]

      ,a.[Contract]
      ,a.[PlanId]
	   ,a.[Age]
		   ,a.AgeRange
	  ,a.[GicCompliance] 
	,a.[GicComplianceRange]
	,a.[AttributeMedicalGroup] 
	,a.[MedGroupLocation] 
     
      ,a.[Race]
		 ,a.[Ethnicity]
		 ,a.[RiskScore]
		   ,a.[RiskScoreRange]
      ,a.[TotalSatisfied]
      ,a.[TotalImpactable]
      ,a.[TotalUnsatisfied]
      ,a.[TotalGap]
      ,a.[CloseGap]
      ,a.[OpenGap]
		   ,b.Code
		   ,b.MemberCurrentCodeStatus
		   ,b.MemberPreviousCodeStatus
		   ,b.MemberCurrentCodeScore
		   ,b.MemberPreviousCodeScore
		   ,c.MeasureScore as MemberCurrentMockChapsCodeScore
		   ,a.MemberCurrentStatus
		   ,a.MemberPreviousStatus
		   ,a.MemberCurrentPredictedScore
		   ,a.MemberPreviousPredictedScore
		   ,a.HasMockCahps
		   ,a.MemberCurrentMockChapsScore
		   ,a.MemberMovingStatus
		   ,a.MonthName
		   ,a.Year
		   ,a.CreatedOn

		     from Chaps_Associate_MemberBasic a
  inner join #all_measure_member_satisfaction_unsatisfaction b
  on   a.MemberId=b.MemberId
  left join #temp_mockcahps_measure_score c on
   b.MemberId=c.MemberId and
  b.Code=c.Code ----------------(0 rows affected)------------





     declare @last_year int=0  
set @last_year=(select max(Year(CreatedOn))
  FROM Chaps_Prediction_MemberCahpsOverview_History)
print(@last_year)
declare @last_month int=0  
set @last_month=(select max(Month)
  FROM Chaps_Prediction_MemberCahpsOverview_History 
   where Year(CreatedOn)=@last_year) 
print(@last_month)
  INSERT INTO [dbo].[Chaps_Status_Moving_Summary]
           ([TotalSatisfied]
           ,[TotalImpactable]
           ,[TotalUnsatisfied]
           ,[SatisfiedToImpactable]
           ,[SatisfiedToUnsatisfied]
           ,[ImpactableToSatisfied]
           ,[ImpactableToUnsatisfied]
           ,[UnsatisfiedToSatisfied]
           ,[UnsatisfiedToImpactable]
           ,[UnChange]
           ,[State]
           ,[StateName]
           ,[County]
 
           ,[Contract]
           ,[PlanId]
		   ,[Age]
		   ,AgeRange
           		   ,[GICComplianceRate] 
	,[GicComplianceRange]
	,[AttributedMedicalGroup] 
	,[MedGroupLocation] 
           ,[Race]
            ,[Ethnicity]
		 ,[RiskScore]
		   ,[RiskScoreRange]
           ,[Race]
		   ,[Ethnicity]
           ,[Month]
           ,[Year]
           ,[CreatedOn]
		   )

	select  TotalSatisfied,
	TotalImpactable,
	TotalUnsatisfied, 
	TotalSatisfiedToImpactable,
	TotalSatisfiedToUnsatisfied,
		TotalImpactableToSatisfied,
	TotalImpactableToUnsatisfied,
 TotalUnsatisfiedToSatisfied,
 TotalUnsatisfiedToImpactable,
 TotalUnchange,
 b.State,
 b.StateName,
 b.County,
 b.Contract,
 b.PlanId,
b.[Age]
		   ,b.AgeRange
           		   ,b.[GicCompliance] 
	,b.[GicComplianceRange]
	,[AttributeMedicalGroup] 
	,[MedGroupLocation] 
           ,b.[Race]
            ,b.[Ethnicity]
		 ,b.[RiskScore]
		   ,[RiskScoreRange]
           ,b.[Race]
		   ,b.[Ethnicity]
 ,@last_month as MonthName
,YEAR(GETDATE()) as Year
,CONVERT(date, GETDATE()) as CreatedOn
from #member_moving_summary a left join Associate_MemberBasic b on a.MemberId =  b.MemberId


  -- History table 

INSERT INTO [dbo].[Chaps_Monthly_ScoreTrend]
           ([Code]
           ,[CurrentPredictedCodeScore]
           ,[PreviousPredictedCodeScore]
           ,[CurrentMockChapsCodeScore]
           ,[MonthlyPredictedScoreTrend]
           ,[Month]
           ,[Year]
           ,[CreatedOn],GicCompliance,[GicComplianceRange],[RiskScore],[RiskScoreRange])
		   select Code,
		   MemberCurrentCodeScore,
		   MemberPreviousCodeScore,
		   MemberCurrentMockChapsCodeScore,
		   MonthlyPredictedScoreTrend,
		   Month,
		    YEAR(GETDATE()) as Year
		   ,CONVERT(date, GETDATE()) as CreatedOn
			from #tempc_chaps_Monthly_ScoreTrend

INSERT INTO [dbo].[Chaps_MemberScore_History]
           ([MemberId]
           ,[StateName]
           ,[State]
           ,[County]
           ,[Contract]

		      ,[Age]
		   ,AgeRange
           		   ,[GicCompliance] 
	,[GicComplianceRange]
	,[AttributeMedicalGroup] 
	,[MedGroupLocation] 
         ,[Race]
		   ,[Ethnicity]
           ,[PlanId]
           ,[MemberCurrentStatus]
           ,[MemberPreviousStatus]
           ,[MemberCurrentPredictedScore]
           ,[MemberPreviousPredictedScore]
           ,[HasMockCahps]
           ,[MemberCurrentMockChapsScore]
           ,[MemberMovingStatus]
           ,[MonthName]
           ,[Year]
           ,[CreatedOn]
          )
		   select 
		   [MemberId]
		   ,[StateName]
		   ,[State]
		   ,[County]
		   ,[Contract]
			,[Age]
		   ,AgeRange
           		   ,[GicCompliance] 
	,[GicComplianceRange]
	,[AttributeMedicalGroup] 
	,[MedGroupLocation] 
         ,[Race]
		   ,[Ethnicity]
           ,[PlanId]
	       ,[MemberCurrentStatus]
           ,[MemberPreviousStatus]
           ,[MemberCurrentPredictedScore]
           ,[MemberPreviousPredictedScore]
           ,[HasMockCahps]
           ,[MemberCurrentMockChapsScore]
           ,[MemberMovingStatus]
           ,[MonthName]
		   ,[Year]
           ,[CreatedOn]
	  
			from Chaps_Associate_MemberBasic


		   
  
  select * from [dbo].[Chaps_Associate_Code_Member_Summary]
  -----------


  select * from [dbo].[Chaps_MemberScore_History_demo] where GicComplianceRange !='Unknown'