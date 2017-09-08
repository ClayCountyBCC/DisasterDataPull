using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DisasterDataPull.Models.Webeoc
{
  public class Form214
  {
    public int incidentid { get; set; }
    public int dataid { get; set; }
    public int prevdataid { get; set; }
    public string position_name { get; set; }
    public DateTime operational_period_from { get; set; }
    public DateTime operational_period_to { get; set; }
    public List<Person> staff { get; set; } = new List<Person>();
    public List<Activity> activities { get; set; } = new List<Activity>();
    public string prepared_by_name { get; set; }
    public string prepared_by_position_title { get; set; }
    public DateTime prepared_by_date_time { get; set; }

    public Form214()
    {

    }

    public List<Form214> Get()
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
            C.dataid IS NULL))";
      

      return Program.Get_Data<Form214>(query, Program.CS_Type.Webeoc);
    }

  }
}
