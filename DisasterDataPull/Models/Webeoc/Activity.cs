using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Dapper;
using System.Data.SqlClient;

namespace DisasterDataPull.Models.Webeoc
{
  public class Activity
  {
    public int dataid { get; set; }
    public int activity_index { get; set; }
    public DateTime activity_date_time { get; set; } = DateTime.MinValue;
    public DateTime activity_date_time_local
    {
      get
      {
        if(activity_date_time == DateTime.MinValue)
        {
          return DateTime.Parse("1/1/2000 12:00 AM");
        } else
        {
          return activity_date_time.ToLocalTime();
        }
      }
    }
    public string activity_notable_activities { get; set; } = "";

    public Activity()
    {

    }

    public static List<Activity> Get()
    {
      var sb = new StringBuilder(@"
        WITH Base214Data AS (
          SELECT 
            MAX(dataid) dataid, 
            prevdataid
          FROM table_260 
          WHERE prevdataid <> 0
          GROUP BY prevdataid
        )
");
      for(int i = 1; i < 25; i++)
      {
        var iStr = i.ToString();
        if (i > 1)
        {
          sb.AppendLine("UNION ALL");
        }
        sb.Append($@"
          SELECT 
            M.dataid,
            { iStr } AS activity_index,
            M.activity_date_time_{ iStr } activity_date_time,
            M.activity_notable_activities_{ iStr } activity_notable_activities
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
              C.dataid IS NULL))
            AND LEN(LTRIM(RTRIM(M.activity_notable_activities_{ iStr }))) > 0
        ");
      }

      return Program.Get_Data<Activity>(sb.ToString(), Program.CS_Type.Webeoc);
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("ActivityData");
      dt.Columns.Add("dataid", typeof(int));
      dt.Columns.Add("activity_index", typeof(int));
      dt.Columns.Add("activity_date_time", typeof(DateTime));
      dt.Columns.Add("activity_notable_activities", typeof(string));
      return dt;
    }

    public static void Merge(List<Activity> al)
    {
      DataTable dt = CreateDataTable();

      foreach (Activity a in al)
      {
        dt.Rows.Add(a.dataid, a.activity_index, a.activity_date_time_local, 
          a.activity_notable_activities.Trim());
      }
      string query = @"        

        SET NOCOUNT, XACT_ABORT ON;
        USE DisasterData;

        MERGE DisasterData.dbo.Activity WITH (HOLDLOCK) AS DDA

        USING @Activity AS A ON DDA.dataid = A.dataid AND 
          DDA.activity_index = A.activity_index

        WHEN MATCHED THEN
          
          UPDATE 
          SET
            activity_date_time=A.activity_date_time,
            activity_notable_activities=A.activity_notable_activities

        WHEN NOT MATCHED BY TARGET THEN

          INSERT 
            (dataid,
            activity_index,
            activity_date_time,
            activity_notable_activities)
          VALUES (
            A.dataid,
            A.activity_index,
            A.activity_date_time,
            A.activity_notable_activities
          )
        WHEN NOT MATCHED BY SOURCE THEN
        
          DELETE;";
      try
      {
        using (IDbConnection db = new SqlConnection(Program.GetCS(Program.CS_Type.DisasterData)))
        {
          db.Execute(query, new { Activity = dt.AsTableValuedParameter("ActivityData") });
        }
      }

      catch (Exception ex)
      {
        new ErrorLog(ex, query);
      }


    }
  }
}
