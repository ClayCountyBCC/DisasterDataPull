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
  public class Form214
  {
    private const Program.CS_Type source = Program.CS_Type.Webeoc;
    private const Program.CS_Type target = Program.CS_Type.DisasterData;
    public int incidentid { get; set; }
    public int dataid { get; set; }
    public int prevdataid { get; set; }
    public string position_name { get; set; } = "";
    public DateTime operational_period_from { get; set; } = DateTime.MinValue;
    public DateTime operational_period_from_local
    {
      get
      {
        if (operational_period_from == DateTime.MinValue)
        {
          return DateTime.Parse("1/1/2000 12:00 AM");
        }
        else
        {
          return operational_period_from.ToLocalTime();
        }
      }
    }
    public DateTime operational_period_to { get; set; } = DateTime.MinValue;
    public DateTime operational_period_to_local
    {
      get
      {
        if (operational_period_to == DateTime.MinValue)
        {
          return DateTime.Parse("1/1/2000 12:00 AM");
        }
        else
        {
          return operational_period_to.ToLocalTime();
        }
      }
    }
    public List<Person214> staff { get; set; } = new List<Person214>();
    public List<Activity214> activities { get; set; } = new List<Activity214>();
    public string prepared_by_name { get; set; } = "";
    public string prepared_by_position_title { get; set; } = "";
    public DateTime prepared_by_date_time { get; set; } = DateTime.MinValue;
    public DateTime prepared_by_date_time_local
    {
      get
      {
        if (prepared_by_date_time == DateTime.MinValue)
        {
          return DateTime.Parse("1/1/2000 12:00 AM");
        }
        else
        {
          return prepared_by_date_time.ToLocalTime();
        }
      }
    }

    public Form214()
    {

    }

    public static List<Form214> Get()
    {
      string query = @"
        WITH Base214Data AS (
          SELECT 
            MAX(dataid) dataid, 
            prevdataid
          FROM table_260 
          WHERE prevdataid <> 0
          GROUP BY prevdataid
        )
        SELECT 
          M.incidentid,
          M.dataid,
          M.prevdataid,
          P.name position_name,
          CAST(CAST(M.operational_period_date_from AS DATE) AS DATETIME) + 
            CAST(CAST(M.operational_period_time_from AS TIME) AS DATETIME) operational_period_from,
          CAST(CAST(M.operational_period_date_to AS DATE) AS DATETIME) + 
            CAST(CAST(M.operational_period_time_to AS TIME) AS DATETIME) operational_period_to,
          M.prepared_by_name,
          M.prepared_by_position_title,
          M.prepared_by_date_time
        FROM table_260 M
        LEFT OUTER JOIN Base214Data B ON M.dataid = B.dataid
        LEFT OUTER JOIN Base214Data C ON M.dataid = C.prevdataid
        LEFT OUTER JOIN Positions P ON M.positionid = P.positionid
        WHERE 
          ((M.prevdataid = 0 AND 
            B.dataid IS NULL AND 
            C.dataid IS NULL) OR 
          (M.prevdataid > 0 AND 
            B.dataid IS NOT NULL AND 
            C.dataid IS NULL))";
      return Program.Get_Data<Form214>(query, source);
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("Form214Data");
      dt.Columns.Add("dataid", typeof(int));
      dt.Columns.Add("prevdataid", typeof(int));
      dt.Columns.Add("incidentid", typeof(int));
      dt.Columns.Add("position_name", typeof(string));
      dt.Columns.Add("operational_period_from", typeof(DateTime));
      dt.Columns.Add("operational_period_to", typeof(DateTime));
      dt.Columns.Add("prepared_by_name", typeof(string));
      dt.Columns.Add("prepared_by_position_title", typeof(string));
      dt.Columns.Add("prepared_by_date_time", typeof(DateTime));
      return dt;
    }

    public static void Merge(List<Form214> fl)
    {
      DataTable dt = CreateDataTable();

      foreach (Form214 f in fl)
      {
        dt.Rows.Add(f.dataid, f.prevdataid, f.incidentid, f.position_name.Trim(),
          f.operational_period_from_local, f.operational_period_to_local, 
          f.prepared_by_name.Trim(), f.prepared_by_position_title.Trim(), 
          f.prepared_by_date_time_local);
      }
      string query = @"        

        SET NOCOUNT, XACT_ABORT ON;
        USE DisasterData;

        MERGE DisasterData.dbo.Form214 WITH (HOLDLOCK) AS DDF

        USING @Form214 AS F ON DDF.dataid = F.dataid 

        WHEN MATCHED THEN
          
          UPDATE 
          SET
            prevdataid=F.prevdataid,
            incidentid=F.incidentid,
            position_name=F.position_name,
            operational_period_from=F.operational_period_from,
            operational_period_to=F.operational_period_to,
            prepared_by_name=F.prepared_by_name,
            prepared_by_position_title=F.prepared_by_position_title,
            prepared_by_date_time=F.prepared_by_date_time

        WHEN NOT MATCHED BY TARGET THEN

          INSERT 
            (dataid,
            prevdataid,
            incidentid,
            position_name,
            operational_period_from,
            operational_period_to,
            prepared_by_name,
            prepared_by_position_title,
            prepared_by_date_time
            )
          VALUES (
            F.dataid,
            F.prevdataid,
            F.incidentid,
            F.position_name,
            F.operational_period_from,
            F.operational_period_to,
            F.prepared_by_name,
            F.prepared_by_position_title,
            F.prepared_by_date_time
          )
        
          WHEN NOT MATCHED BY SOURCE THEN
        
            DELETE;";
      try
      {
        using (IDbConnection db = new SqlConnection(Program.GetCS(target)))
        {
          db.Execute(query, new { Form214 = dt.AsTableValuedParameter("Form214Data") });
        }
      }

      catch (Exception ex)
      {
        new ErrorLog(ex, query);
      }


    }

  }
}
