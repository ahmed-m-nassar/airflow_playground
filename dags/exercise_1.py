
"""
Exercise DAG Specification


Goal:
- echo hello world using bash operator 
- Reconcile rows count from data/source_a.csv & data/source_b.csv 
- If all counts are equal → send a success email
  If any counts differ → send a failure email

echo hello world (BashOperator)
  |
compare_counts (BranchPythonOperator)
      /           \
success_email    failure_email

DAG configurations : 
- Name it "reconciliation_exercise_1"
- schedule it to run daily 
"""