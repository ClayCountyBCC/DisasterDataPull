using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DisasterDataPull.Models.Webeoc
{
  public class WebeocActivity
  {
    public int dataid { get; set; }
    public int index { get; set; }
    public int activity_date_time { get; set; }
    public int activity_notable_activities { get; set; }

    public WebeocActivity()
    {

    }

    public static List<WebeocActivity> Get()
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
            0 as index,
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
            AND (LEN(LTRIM(RTRIM(M.activity_date_time_{ iStr }))) > 0 OR
              LEN(LTRIM(RTRIM(M.activity_notable_activities_{ iStr }))) > 0)
        ");


      }


    }
  }
}
