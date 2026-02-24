SELECT 
    DD.DDID,
    DD.SDID,
    DD.FORMATID,
    CAST(DD.DATA AS VARCHAR(MAX)) AS DATA,
    DD.USERID,
    DD.LINKID,
    DD.nd_auto_increment_id,
    D.PID
INTO DOCDATA2_temp_suven
FROM DOCDATA2 DD
JOIN DOCUMENT D ON DD.SDID = D.SDID
JOIN (
    SELECT DISTINCT PatientProfile.pid
    FROM PatientProfile
    JOIN PROBLEM ON PROBLEM.Pid = PatientProfile.pid
    LEFT JOIN PatientVisit pv ON pv.PatientProfileId = PatientProfile.PatientProfileId
    LEFT JOIN PatientVisitDiags pd ON pd.PatientVisitId = pv.PatientVisitId
    JOIN MasterDiagnosis ON MasterDiagnosis.MasterDiagnosisid = PROBLEM.icd10MasterDiagnosisId
    WHERE PROBLEM.icd10MasterDiagnosisId IS NOT NULL
      AND MasterDiagnosis.CODE IN ('G30','G30.0','G30.1','G30.8','G30.9','331.0','331.00')
) P ON D.PID = P.PID;


SELECT 
    DD.*,
    D.PID
INTO DOCDATA_temp_suven
FROM DOCDATA DD
JOIN DOCUMENT D ON DD.SDID = D.SDID
JOIN (
    SELECT DISTINCT PatientProfile.pid
    FROM PatientProfile  
    JOIN PROBLEM ON PROBLEM.Pid = PatientProfile.pid
    LEFT JOIN PatientVisit pv ON pv.PatientProfileId = PatientProfile.PatientProfileId
    LEFT JOIN PatientVisitDiags pd ON pd.PatientVisitId = pv.PatientVisitId
    JOIN MasterDiagnosis ON MasterDiagnosis.MasterDiagnosisid = PROBLEM.icd10MasterDiagnosisId
    WHERE PROBLEM.icd10MasterDiagnosisId IS NOT NULL
      AND MasterDiagnosis.CODE IN ('G30','G30.0','G30.1','G30.8','G30.9','331.0','331.00')
) PPID ON D.PID = PPID.PID;
