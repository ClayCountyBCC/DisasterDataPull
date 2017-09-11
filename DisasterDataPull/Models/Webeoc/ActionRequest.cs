using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Data;
using System.Data.SqlClient;

namespace DisasterDataPull.Models.Webeoc
{
  public class ActionRequest
  {
    private const Program.CS_Type source = Program.CS_Type.Webeoc;
    private const Program.CS_Type target = Program.CS_Type.DisasterData;
    public int dataid { get; set; }
    public int prevdataid { get; set; }
    public string incident_name { get; set; } = "";
    public string position_name { get; set; } = "";
    public DateTime entry_date { get; set; } = DateTime.MinValue;
    public DateTime entry_date_local
    {
      get
      {
        if (entry_date == DateTime.MinValue)
        {
          return DateTime.Parse("1/1/2000 12:00 AM");
        }
        else
        {
          return entry_date.ToLocalTime();
        }
      }
    }
    public string tracking_number { get; set; }
    public string priority { get; set; }
    public string assigned_to { get; set; }
    public string short_task_desc { get; set; }
    public string task_desc { get; set; }

    public ActionRequest()
    {

    }

    public static List<ActionRequest> Get()
    {
      string query = @"
        USE wedb_7;
        SELECT           
          M.dataid,
          M.prevdataid,
          I.name incident_name,
          P.name position_name,
          M.entrydate entry_date,
          M.tracking_number,
          M.d_1 priority,
          M.assigned_to,
          M.task_desc_short short_task_desc,
          M.task_description task_desc
        FROM table_422 M
        LEFT OUTER JOIN Positions P ON M.positionid = P.positionid
        LEFT OUTER JOIN Incidents I ON I.incidentid = M.incidentid
        WHERE 
          M.prevdataid=0";
      return Program.Get_Data<ActionRequest>(query, source);
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("ActionRequestData");
      dt.Columns.Add("dataid", typeof(int));
      dt.Columns.Add("prevdataid", typeof(int));
      dt.Columns.Add("incident_name", typeof(string));
      dt.Columns.Add("position_name", typeof(string));
      dt.Columns.Add("entry_date", typeof(DateTime));
      dt.Columns.Add("tracking_number", typeof(string));
      dt.Columns.Add("priority", typeof(string));
      dt.Columns.Add("assigned_to", typeof(string));
      dt.Columns.Add("short_task_desc", typeof(string));
      dt.Columns.Add("task_desc", typeof(string));
      return dt;
    }

    public static void Merge(List<ActionRequest> arl)
    {
      DataTable dt = CreateDataTable();

      foreach (ActionRequest a in arl)
      {
        dt.Rows.Add(a.dataid, a.prevdataid, a.incident_name.Trim(),
          a.position_name.Trim(), a.entry_date_local, a.tracking_number.Trim(), 
          a.priority.Trim(), a.assigned_to.Trim(), a.short_task_desc.Trim(), 
          a.task_desc.Trim());
      }
      string query = @"        

        SET NOCOUNT, XACT_ABORT ON;
        USE DisasterData;

        MERGE DisasterData.dbo.ActionRequest WITH (HOLDLOCK) AS DDAR

        USING @Request AS R ON DDAR.dataid = R.dataid

        WHEN MATCHED THEN
          
          UPDATE 
          SET
            prevdataid=R.prevdataid,
            incident_name=R.incident_name,
            position_name=R.position_name,
            entry_date=R.entry_date,
            tracking_number=R.tracking_number,
            priority=R.priority,
            assigned_to=R.assigned_to,
            short_task_desc=R.short_task_desc,
            task_desc=R.task_desc

        WHEN NOT MATCHED BY TARGET THEN

          INSERT 
            (
              dataid,
              prevdataid,
              incident_name,
              position_name,
              entry_date,
              tracking_number,
              priority,
              assigned_to,
              short_task_desc,
              task_desc
            )
          VALUES 
            (
              R.dataid,
              R.prevdataid,
              R.incident_name,
              R.position_name,
              R.entry_date,
              R.tracking_number,
              R.priority,
              R.assigned_to,
              R.short_task_desc,
              R.task_desc
            )

        WHEN NOT MATCHED BY SOURCE THEN
        
          DELETE;";
      try
      {
        using (IDbConnection db = new SqlConnection(Program.GetCS(target)))
        {
          db.Execute(query, new { Request = dt.AsTableValuedParameter("ActionRequestData") });
        }
      }

      catch (Exception ex)
      {
        new ErrorLog(ex, query);
      }


    }
  }
}
