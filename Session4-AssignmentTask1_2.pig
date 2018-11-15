/* (a) Top 5 employees (employee id and employee name) with highest rating. (In case two
employees have same rating, employee with name coming first in dictionary should get
preference)*/

employees = load 'employee_details.txt' Using PigStorage(',') as (empID:int,name:Chararray,salary:int,departmentID:int);
topEmp = order employees by salary desc, name asc;
emp = foreach topEmp generate empID, name;
top5 = limit emp 5;
DUMP top5;


/* (b) Top 3 employees (employee id and employee name) with highest salary, whose employee id
is an odd number. (In case two employees have same salary, employee with name coming first
in dictionary should get preference) */

employees = load 'employee_details.txt' Using PigStorage(',') as (empID:int,name:Chararray,salary:int,departmentID:int);
filterEmp = filter employees by empID%2!=0;
topEmp = order filterEmp by salary desc, name asc;
emp = foreach topEmp generate empID, name;
top3 = limit emp 3;
DUMP top3;


/* (c) Employee (employee id and employee name) with maximum expense (In case two employees have same expense, employee with name coming first in dictionary should get preference)*/

employees = load 'employee_details.txt' Using PigStorage(',') as (empID:int,name:Chararray,salary:int,departmentID:int);
expenses = load 'employee_expenses.txt' Using PigStorage() as (empID:int,expense:int);
joinedEmp = join employees by empID, expenses by empID;
orderedExpenses = order joinedEmp by expenses::expense desc, employees::name asc;
topEmp = foreach orderedExpenses generate employees::empID, employees::name;
mostExpense = limit topEmp 1;
DUMP mostExpense;



/* (d) List of employees (employee id and employee name) having entries in employee_expenses file.*/

employees = load 'employee_details.txt' Using PigStorage(',') as (empID:int,name:Chararray,salary:int,departmentID:int);
expenses = load 'employee_expenses.txt' Using PigStorage() as (empID:int,expense:int);
joinedEmp = join employees by empID, expenses by empID;
emp = foreach orderedExpenses generate employees::empID, employees::name;
distinctEmp = DISTINCT emp;
DUMP distinctEmp;

/* (e) List of employees (employee id and employee name) having no entry in employee_expenses file.*/

employees = load 'employee_details.txt' Using PigStorage(',') as (empID:int,name:Chararray,salary:int,departmentID:int);
expenses = load 'employee_expenses.txt' Using PigStorage() as (empID:int,expense:int);
joinedEmpLeft = join employees by empID LEFT OUTER, expenses by empID;
filterEmp = filter joinedEmpLeft by expenses::expense is null
emp = foreach filterEmp generate employees::empID, employees::name;
distinctEmp = DISTINCT emp;
DUMP distinctEmp;