using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DisasterDataPull.Models.Webeoc
{
  public class WebeocPerson
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
    public WebeocPerson()
    {

    }
  }
}
