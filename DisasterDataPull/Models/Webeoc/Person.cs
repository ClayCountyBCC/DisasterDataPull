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
  public class Person
  {
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
        if(name.Length > 4)
        {
          if (int.TryParse(name.Substring(0, 4), out int i))
          {
            return i;
          }
        }
        return -1;
      }
    }
    public Person()
    {

    }

    public static List<Person> Get()
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
        SELECT 
          M.dataid,
          0 as person_index,
          M.ics_position,
          M.name,
          M.home_agency   
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
");
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
            AND (LEN(LTRIM(RTRIM(M.ics_position_{ iStr }))) > 0 OR
              LEN(LTRIM(RTRIM(M.name_{ iStr }))) > 0 OR
              LEN(LTRIM(RTRIM(M.home_agency_{ iStr }))) > 0)
        ");
      }

      return Program.Get_Data<Person>(sb.ToString(), Program.CS_Type.Webeoc);
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("PersonData");
      dt.Columns.Add("dataid", typeof(int));
      dt.Columns.Add("person_index", typeof(int));      
      dt.Columns.Add("name", typeof(string));
      dt.Columns.Add("ics_position", typeof(string));
      dt.Columns.Add("home_agency", typeof(string));
      dt.Columns.Add("employee_id", typeof(int));
      return dt;
    }

    public static void Merge(List<Person> pl)
    {
      DataTable dt = CreateDataTable();

      foreach (Person p in pl)
      {
        dt.Rows.Add(p.dataid, p.person_index, p.name.Trim(), 
          p.ics_position.Trim(), p.home_agency.Trim(), p.employee_id);
      }
      string query = @"        

        SET NOCOUNT, XACT_ABORT ON;
        USE DisasterData;

        MERGE DisasterData.dbo.Person WITH (HOLDLOCK) AS DDP

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
        using (IDbConnection db = new SqlConnection(Program.GetCS(Program.CS_Type.DisasterData)))
        {
          db.Execute(query, new { Person = dt.AsTableValuedParameter("PersonData") });
        }
      }

      catch (Exception ex)
      {
        new ErrorLog(ex, query);
      }


    }
  }
}
