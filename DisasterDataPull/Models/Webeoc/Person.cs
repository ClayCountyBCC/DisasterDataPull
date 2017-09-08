using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DisasterDataPull.Models.Webeoc
{
  public class Person
  {
    public int dataid { get; set; }
    public int index { get; set; }
    public string name { get; set; }
    public string ics_position { get; set; }
    public string home_agency { get; set; }
    public int employee_id
    {
      get
      {
        return 0; // grab that here.
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
          M.prevdataid,
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
            { iStr } AS index,
            M.ics_position_{ iStr } ics_position
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


  }
}
