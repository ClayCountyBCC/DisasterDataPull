using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DisasterDataPull.Models.Webeoc
{
  public class CompletedForm
  {
    public int incidentid { get; set; }
    public int dataid { get; set; }
    public int prevdataid { get; set; }
    public string position_name { get; set; }
    public DateTime operational_period_from { get; set; }
    public DateTime operational_period_to { get; set; }
    public List<WebeocPerson> staff { get; set; } = new List<WebeocPerson>();
    public List<WebeocActivity> activities { get; set; } = new List<WebeocActivity>();
    public string prepared_by_name { get; set; }
    public string prepared_by_position_title { get; set; }
    public DateTime prepared_by_date_time { get; set; }

    public CompletedForm()
    {

    }
  }
}
