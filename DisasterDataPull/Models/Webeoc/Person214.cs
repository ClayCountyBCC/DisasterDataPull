﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Data;
using System.Data.SqlClient;

namespace DisasterDataPull.Models.Webeoc
{
  public class Person214
  {
    private const Program.CS_Type source = Program.CS_Type.Webeoc;
    private const Program.CS_Type target = Program.CS_Type.DisasterData;
    public int dataid { get; set; }
    public int person_index { get; set; }
    public string name { get; set; } = "";
    public string ics_position { get; set; } = "";
    public string home_agency { get; set; } = "";
    public int employee_id
    {
      get
      {
        name = name.Trim();
        if (name.Length > 4)
        {
          if (int.TryParse(name.Substring(0, 5), out int i))
          {
            return (i < 1000) ? i - i + 9999 : i;
          }
        }
        if (name.Length > 3 && name.Substring(0, 1).ToUpper() == "V")
        {
          if (int.TryParse(name.Substring(1, 3), out int b))
          {
            return b - b + 9999;
          }
        }

        if (name.Length > 3)
        {
          if (int.TryParse(name.Substring(0, 4), out int j))
          {
            return j;
          }
        }

        return -1;
      }
    }
    public Person214()
    {

    }

    public static List<Person214> Get()
    {
      var sb = new StringBuilder(@"
        SELECT 
          M.dataid,
          0 as person_index,
          M.ics_position,
          M.name,
          M.home_agency   
        FROM table_260 M
        WHERE 
          M.prevdataid=0");
      for (int i = 1; i < 9; i++)
      {
        var iStr = i.ToString();
        sb.AppendLine("UNION ALL");
        sb.Append($@"
          SELECT 
            M.dataid,
            { iStr } AS person_index,
            M.ics_position_{ iStr } ics_position,
            M.name_{ iStr } name,
            M.home_agency_{ iStr } name
          FROM table_260 M
          WHERE 
            M.prevdataid=0
            AND (LEN(LTRIM(RTRIM(M.ics_position_{ iStr }))) > 0 OR
              LEN(LTRIM(RTRIM(M.name_{ iStr }))) > 0 OR
              LEN(LTRIM(RTRIM(M.home_agency_{ iStr }))) > 0)
        ");
      }

      return Program.Get_Data<Person214>(sb.ToString(), source);
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("Person214Data");
      dt.Columns.Add("dataid", typeof(int));
      dt.Columns.Add("person_index", typeof(int));      
      dt.Columns.Add("name", typeof(string));
      dt.Columns.Add("ics_position", typeof(string));
      dt.Columns.Add("home_agency", typeof(string));
      dt.Columns.Add("employee_id", typeof(int));
      return dt;
    }

    public static void Merge(List<Person214> pl)
    {
      DataTable dt = CreateDataTable();

      foreach (Person214 p in pl)
      {
        dt.Rows.Add(p.dataid, p.person_index, p.name.Trim(), 
          p.ics_position.Trim(), p.home_agency.Trim(), p.employee_id);
      }
      string query = @"        

        SET NOCOUNT, XACT_ABORT ON;
        USE DisasterData;

        MERGE DisasterData.dbo.Person214 WITH (HOLDLOCK) AS DDP

        USING @Person AS P ON DDP.dataid = P.dataid AND 
          DDP.person_index = P.person_index

        WHEN MATCHED THEN
          
          UPDATE 
          SET
            [name]=P.name,
            ics_position=P.ics_position,
            home_agency=P.home_agency,
            employee_id=P.employee_id

        WHEN NOT MATCHED BY TARGET THEN

          INSERT 
            (dataid,
            person_index,
            [name],
            ics_position,
            home_agency,
            employee_id)
          VALUES (
            P.dataid,
            P.person_index,
            P.name,
            P.ics_position,
            P.home_agency,
            P.employee_id
          )

        WHEN NOT MATCHED BY SOURCE THEN
        
          DELETE;";
      try
      {
        using (IDbConnection db = new SqlConnection(Program.GetCS(target)))
        {
          db.Execute(query, new { Person = dt.AsTableValuedParameter("Person214Data") });
        }
      }

      catch (Exception ex)
      {
        new ErrorLog(ex, query);
      }


    }
  }
}
