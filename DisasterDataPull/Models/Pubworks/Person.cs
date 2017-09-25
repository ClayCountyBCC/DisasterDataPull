using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Data;
using System.Data.SqlClient;

namespace DisasterDataPull.Models.Pubworks
{
  public class Person
  {
    private const Program.CS_Type source = Program.CS_Type.TSPW;
    private const Program.CS_Type target = Program.CS_Type.DisasterData;
    public int work_order_id { get; set; }
    public int activity_id { get; set; }
    public DateTime activity_date { get; set; }
    public string activity_name { get; set; } = "";
    public string department_name { get; set; } = "";
    public string location_name { get; set; } = "";
    public string work_order_name { get; set; } = "";
    public string task_code { get; set; }
    public string task_name { get; set; } = "";
    public int employee_id { get; set; }
    public string employee_name { get; set; } = "";
    public Person ()
    {

    }

    public static List<Person> Get()
    {
      string query = @"
        USE PubWorks;          
        SELECT             
          Activities.WOID work_order_id,           
          Activities.ID activity_id,           
          CAST(Activities.Date AS DATE) activity_date,            
          Activities.Name activity_name,            
          Department.Name department_name,          
          Locations.Name location_name,            
          WorkOrders.Name work_order_name,            
          Tasks.Code task_code,            
          Tasks.Name task_name,            
          CAST(
            CASE WHEN ISNUMERIC(Employees.Code) != 1 
            THEN REPLACE(Employees.Code, 'Inmt', '9')
            ELSE Employees.Code 
            END 
            AS INT) employee_id,           
            LTRIM(RTRIM(Employees.Name)) + ', ' + 
            LTRIM(RTRIM(Employees.FirstName)) employee_name          
        FROM Activities           
        LEFT OUTER JOIN WorkOrders ON WorkOrders.ID = Activities.WOID          
        LEFT OUTER JOIN Locations ON Activities.LocID = Locations.ID           
        LEFT OUTER JOIN Tasks ON Activities.TskID = Tasks.ID           
        LEFT OUTER JOIN Employees ON Activities.EmpID = Employees.ID          
        LEFT OUTER JOIN Department ON Activities.DeptID = Department.ID         
        WHERE Activities.WOID=39";
      return Program.Get_Data<Person>(query, source);
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("PersonPubworksData");
      
      dt.Columns.Add("activity_id", typeof(int));
      dt.Columns.Add("activity_date", typeof(DateTime));
      dt.Columns.Add("activity_name", typeof(string));
      dt.Columns.Add("work_order_id", typeof(int));
      dt.Columns.Add("work_order_name", typeof(string));
      dt.Columns.Add("department_name", typeof(string));
      dt.Columns.Add("location_name", typeof(string));
      dt.Columns.Add("task_code", typeof(string));
      dt.Columns.Add("task_name", typeof(string));
      dt.Columns.Add("employee_id", typeof(int));
      dt.Columns.Add("employee_name", typeof(string));
      return dt;
    }

    public static void Merge(List<Person> pl)
    {
      DataTable dt = CreateDataTable();
      foreach (Person p in pl)
      {
        dt.Rows.Add(p.activity_id, p.activity_date,
          p.activity_name.Trim(), p.work_order_id, p.work_order_name.Trim(),
          p.department_name.Trim(), p.location_name.Trim(),
          p.task_code.Trim(), p.task_name.Trim(),
          p.employee_id, p.employee_name.Trim());
      }

      string query = @"        

        SET NOCOUNT, XACT_ABORT ON;
        USE DisasterData;

        MERGE DisasterData.dbo.PersonPubworks WITH (HOLDLOCK) AS PPW

        USING @Person AS P ON PPW.activity_id=P.activity_id

        WHEN MATCHED THEN
          
          UPDATE 
          SET
            activity_date=P.activity_date
            ,activity_name=P.activity_name
            ,work_order_id=P.work_order_id
            ,work_order_name=P.work_order_name
            ,department_name=P.department_name
            ,location_name=P.location_name
            ,task_code=P.task_code
            ,task_name=P.task_name
            ,employee_id=P.employee_id
            ,employee_name=P.employee_name

        WHEN NOT MATCHED BY TARGET THEN

          INSERT 
            (
              activity_id
              ,activity_date
              ,activity_name
              ,work_order_id
              ,work_order_name
              ,department_name
              ,location_name
              ,task_code
              ,task_name
              ,employee_id
              ,employee_name
            )
          VALUES 
            (
              P.activity_id
              ,P.activity_date
              ,P.activity_name
              ,P.work_order_id
              ,P.work_order_name
              ,P.department_name
              ,P.location_name
              ,P.task_code
              ,P.task_name
              ,P.employee_id
              ,P.employee_name
            )

        WHEN NOT MATCHED BY SOURCE THEN
        
          DELETE;";
      try
      {
        using (IDbConnection db = new SqlConnection(Program.GetCS(target)))
        {
          db.Execute(query, new { Person = dt.AsTableValuedParameter("PersonPubworksData") });
        }
      }

      catch (Exception ex)
      {
        new ErrorLog(ex, query);
      }


    }


  }
}
