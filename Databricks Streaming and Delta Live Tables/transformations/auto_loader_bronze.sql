CREATE OR REFRESH STREAMING TABLE guns.bronze.stress_level_bronze 
AS 
SELECT 
  * 
FROM STREAM read_files(
  '/Volumes/guns/data/stress_level', 
  format => 'csv', 
  header => 'true',
  schema => 'anxiety_level int, self_esteem int, mental_health_history int, depression int, headache int, blood_pressure int, sleep_quality int, breathing_problem int, noise_level int, living_conditions int, safety int, basic_needs int, academic_performance int, study_load int, teacher_student_relationship int, future_career_concerns int, social_support int, peer_pressure int, extracurricular_activities int, bullying int, stress_level int'
);