﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Dapper;
using System.Data.SqlClient;

namespace DisasterDataPull.Models.Webeoc
{
  public class Activity214
  {
    private const Program.CS_Type source = Program.CS_Type.Webeoc;
    private const Program.CS_Type target = Program.CS_Type.DisasterData;
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
          var mor = TimeZoneInfo.FindSystemTimeZoneById("Morocco Standard Time");
          var est = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
          return TimeZoneInfo.ConvertTime(activity_date_time, mor, est);
          //return TimeZoneInfo.ConvertTimeFromUtc(activity_date_time, );
          //return activity_date_time.ToLocalTime();
        }
      }
    }
    public string category
    {
      get
      {

        string s = activity_notable_activities.Trim();
        if (s.Length < 2) return "";
        s = s.Substring(0, 2);
        if (s.Substring(1, 1) == "-")
        {
          return s.Substring(0, 1);
        }
        else
        {
          return "";
        }
        
      }
    }

    public string activity_notable_activities { get; set; } = "";

    public Activity214()
    {

    }

    public static List<Activity214> Get()
    {
      var sb = new StringBuilder("");
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
          WHERE 
            M.prevdataid = 0
            AND LEN(LTRIM(RTRIM(M.activity_notable_activities_{ iStr }))) > 0
        ");
      }

      return Program.Get_Data<Activity214>(sb.ToString(), source);
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("Activity214Data");
      dt.Columns.Add("dataid", typeof(int));
      dt.Columns.Add("activity_index", typeof(int));
      dt.Columns.Add("activity_date_time", typeof(DateTime));
      dt.Columns.Add("activity_notable_activities", typeof(string));
      dt.Columns.Add("category", typeof(string));
      return dt;
    }

    public static void Merge(List<Activity214> al)
    {
      DataTable dt = CreateDataTable();

      foreach (Activity214 a in al)
      {
        dt.Rows.Add(a.dataid, a.activity_index, a.activity_date_time_local, 
          a.activity_notable_activities.Trim(), a.category);
      }
      string query = @"        

        SET NOCOUNT, XACT_ABORT ON;
        USE DisasterData;

        MERGE DisasterData.dbo.Activity214 WITH (HOLDLOCK) AS DDA

        USING @Activity AS A ON DDA.dataid = A.dataid AND 
          DDA.activity_index = A.activity_index

        WHEN MATCHED THEN
          
          UPDATE 
          SET
            activity_date_time=A.activity_date_time,
            activity_notable_activities=A.activity_notable_activities,
            category=A.category

        WHEN NOT MATCHED BY TARGET THEN

          INSERT 
            (dataid,
            activity_index,
            activity_date_time,
            activity_notable_activities,
            category)
          VALUES (
            A.dataid,
            A.activity_index,
            A.activity_date_time,
            A.activity_notable_activities,
            A.category
          )
        WHEN NOT MATCHED BY SOURCE THEN
        
          DELETE;";
      try
      {
        using (IDbConnection db = new SqlConnection(Program.GetCS(target)))
        {
          db.Execute(query, new { Activity = dt.AsTableValuedParameter("Activity214Data") });
        }
      }

      catch (Exception ex)
      {
        new ErrorLog(ex, query);
      }


    }
  }
}
