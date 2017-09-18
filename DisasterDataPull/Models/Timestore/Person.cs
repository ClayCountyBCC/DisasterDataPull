using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Data;
using System.Data.SqlClient;

namespace DisasterDataPull.Models.Timestore
{
  public class Person
  {
    private const Program.CS_Type source = Program.CS_Type.TSPW;
    private const Program.CS_Type target = Program.CS_Type.DisasterData;
    public int employee_id { get; set; }
    public DateTime work_date { get; set; }
    public string dept_id { get; set; }
    public string disaster_name { get; set; }
    public DateTime pay_period_ending { get; set; }
    public string comment { get; set; }
    public double disaster_work_hours { get; set; }
    public string disaster_work_times { get; set; }
    public Person()
    {

    }

    public static List<Person> Get()
    {
      string query = @"
        USE TimeStore;
        SELECT 
          D.Name disaster_name,
          employee_id,
          dept_id,
          work_date,
          pay_period_ending,
          comment,
          disaster_work_hours,
          disaster_work_times
        FROM Work_Hours W
        INNER JOIN Disaster_Data D ON W.work_date BETWEEN D.Disaster_Start AND D.Disaster_End
        WHERE 
          W.disaster_work_hours > 0 OR 
          LEN(disaster_work_times) > 0";
      return Program.Get_Data<Person>(query, source);
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("PersonTimeStoreData");
      dt.Columns.Add("employee_id", typeof(int));
      dt.Columns.Add("work_date", typeof(DateTime));
      dt.Columns.Add("disaster_name", typeof(string));
      dt.Columns.Add("dept_id", typeof(string));
      dt.Columns.Add("pay_period_ending", typeof(DateTime));
      dt.Columns.Add("comment", typeof(string));
      dt.Columns.Add("disaster_work_hours", typeof(float));
      dt.Columns.Add("disaster_work_times", typeof(string));
      return dt;
    }

    public static void Merge(List<Person> pl)
    {
      DataTable dt = CreateDataTable();

      foreach (Person p in pl)
      {
        dt.Rows.Add(p.employee_id, p.work_date, p.disaster_name,
          p.dept_id, p.pay_period_ending, p.comment.Trim(),
          p.disaster_work_hours, p.disaster_work_times);
      }
      string query = @"        

        SET NOCOUNT, XACT_ABORT ON;
        USE DisasterData;

        MERGE DisasterData.dbo.PersonTimeStore WITH (HOLDLOCK) AS PTS

        USING @Person AS P ON PTS.employee_id = P.employee_id AND 
          PTS.work_date = P.work_date

        WHEN MATCHED THEN
          
          UPDATE 
          SET
            disaster_name=P.disaster_name,
            dept_id=P.dept_id,
            pay_period_ending=P.pay_period_ending,
            comment=P.comment,
            disaster_work_hours=P.disaster_work_hours,
            disaster_work_times=P.disaster_work_times


        WHEN NOT MATCHED BY TARGET THEN

          INSERT 
            (
              employee_id,
              work_date,
              disaster_name,
              dept_id,
              pay_period_ending,
              comment,
              disaster_work_hours,
              disaster_work_times
            )
          VALUES 
            (
              P.employee_id,
              P.work_date,
              P.disaster_name,
              P.dept_id,
              P.pay_period_ending,
              P.comment,
              P.disaster_work_hours,
              P.disaster_work_times
            )

        WHEN NOT MATCHED BY SOURCE THEN
        
          DELETE;";
      try
      {
        using (IDbConnection db = new SqlConnection(Program.GetCS(target)))
        {
          db.Execute(query, new { Person = dt.AsTableValuedParameter("PersonTimeStoreData") });
        }
      }

      catch (Exception ex)
      {
        new ErrorLog(ex, query);
      }


    }

  }
}
