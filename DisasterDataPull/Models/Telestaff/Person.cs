using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Data;
using System.Data.SqlClient;

namespace DisasterDataPull.Models.Telestaff
{
  class Person
  {

    private const Program.CS_Type source = Program.CS_Type.TSPW;
    private const Program.CS_Type target = Program.CS_Type.DisasterData;
    public int staffing_id { get; set; }
    public string disaster_name { get; set; }
    public DateTime work_date { get; set; }
    public string work_type { get; set; }
    public double staffing_hours { get; set; }
    public string employee_name { get; set; }
    public int employee_id { get; set; }
    public DateTime staffing_start_date { get; set; }
    public DateTime staffing_end_date { get; set; }
    public string payroll_code { get; set; }
    public double pay_period_hours { get; set; }
    public string comment { get; set; }
    public string unit { get; set; }
    public string unit_name { get; set; }
    public int payroll_rule { get; set; } = 0;
    public double pay_rate { get; set; }
    public Person()
    {

    }

    public static List<Person> Get()
    {
      string query = @"
        USE Telestaff;

        SELECT 
          Staffing_Tbl.staffing_no_in staffing_id,
          DD.Name disaster_name,
          ISNULL(DPR.rule_applied, 0) payroll_rule,
          Resource_Tbl.rsc_hourwage_db pay_rate,
          Staffing_Tbl.Staffing_Calendar_Da work_date, 
          SUM(DATEDIFF(minute,Staffing_Tbl.Staffing_Start_Dt,Staffing_Tbl.Staffing_End_Dt))/60.00 as staffing_hours, 
          Resource_Master_Tbl.RscMaster_Name_Ch employee_name,
          Resource_Master_Tbl.RscMaster_EmployeeID_Ch employee_id, 
          Staffing_Tbl.Staffing_Start_Dt staffing_start_date,
          Staffing_Tbl.Staffing_End_Dt staffing_end_date,
          Wstat_Cde_Tbl.Wstat_Name_Ch work_type,
          Wstat_Payroll_ch payroll_code,
          Pay_Information_Tbl.PayInfo_FlsaHours_In pay_period_hours,
          ISNULL(Staffing_tbl.Staffing_Note_Vc, '') comment,
          Unit_Tbl.unit_abrv_ch unit,
          Unit_Tbl.unit_name_ch unit_name
        FROM Staffing_Tbl 
        LEFT OUTER JOIN strategy_tbl STRAT ON Staffing_tbl.strat_no_in=STRAT.strat_no_in 
        JOIN Resource_Tbl ON Resource_Tbl.Rsc_No_In=Staffing_Tbl.Rsc_No_In 
        JOIN Wstat_Cde_Tbl ON Wstat_Cde_Tbl.Wstat_No_In=Staffing_Tbl.Wstat_No_In 
        JOIN Shift_Tbl ON Shift_Tbl.Shift_No_In=Staffing_Tbl.Shift_No_In 
        JOIN Wstat_Type_Tbl ON Wstat_Type_Tbl.Wstat_Type_No_In=Wstat_Cde_Tbl.Wstat_Type_No_In 
        LEFT OUTER JOIN Pay_Information_Tbl ON Pay_Information_Tbl.PayInfo_No_In=Resource_Tbl.PayInfo_No_In 
        JOIN Resource_Master_Tbl ON Resource_Master_Tbl.RscMaster_No_In=Resource_Tbl.RscMaster_No_In 
        JOIN Position_Tbl ON Position_Tbl.Pos_No_In=Staffing_Tbl.Pos_No_In         
        JOIN Unit_Tbl ON Unit_Tbl.Unit_No_In=Position_Tbl.Unit_No_In 
        JOIN Station_Tbl ON Station_Tbl.Station_No_In=Unit_Tbl.Station_No_In   
        INNER JOIN TimeStore.dbo.Disaster_Data DD ON staffing_calendar_da BETWEEN DD.Disaster_Start AND DD.Disaster_End
        LEFT OUTER JOIN TimeStore.dbo.Disaster_Pay_Rules DPR ON staffing_calendar_da = DPR.disaster_date
        WHERE 
          Station_Tbl.Region_No_In IN (4,2,5,6)       
          AND Wstat_Payroll_ch IS NOT NULL
          --AND Wstat_Payroll_ch NOT IN ('100', '101', '110', '090', '111', '123')
          --AND Wstat_Cde_Tbl.Wstat_Name_Ch NOT IN ('Admin Leave')
          AND Wstat_Cde_Tbl.Wstat_Abrv_Ch NOT IN ('OTR', 'OTRR', 'ORD', 'ORRD', 'NO', 'DPRN') 
          AND (Resource_Master_Tbl.RscMaster_Thru_Da IS NULL OR 
            Resource_Master_Tbl.RscMaster_Thru_Da >= DD.Disaster_Start) 
        GROUP BY
          DD.Name,
          Unit_Tbl.unit_abrv_ch,
          Unit_Tbl.unit_name_ch,
          DPR.rule_applied,
          Resource_Tbl.rsc_hourwage_db,
          Staffing_Tbl.Staffing_Calendar_Da,
          Staffing_Tbl.staffing_no_in,
          Resource_Master_Tbl.RscMaster_Name_Ch,
          Resource_Master_Tbl.RscMaster_EmployeeID_Ch,
          Wstat_Type_Tbl.Wstat_Type_No_In,
          Wstat_Cde_Tbl.Wstat_No_In,
          Wstat_Cde_Tbl.Wstat_Name_Ch,
          Wstat_Cde_Tbl.Wstat_Payroll_Ch,
          Staffing_tbl.Staffing_Note_Vc,
          Pay_Information_Tbl.PayInfo_FlsaHours_In,
          Staffing_Tbl.Staffing_Start_Dt,
          Staffing_Tbl.Staffing_End_Dt";
      return Program.Get_Data<Person>(query, source);
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("PersonTelestaffData");
      dt.Columns.Add("staffing_id", typeof(int));
      dt.Columns.Add("disaster_name", typeof(string));
      dt.Columns.Add("work_date", typeof(DateTime));
      dt.Columns.Add("staffing_hours", typeof(float));
      dt.Columns.Add("employee_name", typeof(string));
      dt.Columns.Add("employee_id", typeof(int));
      dt.Columns.Add("staffing_start_date", typeof(DateTime));
      dt.Columns.Add("staffing_end_date", typeof(DateTime));
      dt.Columns.Add("payroll_code", typeof(string));
      dt.Columns.Add("pay_period_hours", typeof(float));
      dt.Columns.Add("comment", typeof(string));
      dt.Columns.Add("unit", typeof(string));
      dt.Columns.Add("work_type", typeof(string));
      dt.Columns.Add("unit_name", typeof(string));
      dt.Columns.Add("payroll_rule", typeof(int));
      dt.Columns.Add("pay_rate", typeof(float));
      return dt;
    }

    public static void Merge(List<Person> pl)
    {
      DataTable dt = CreateDataTable();

      foreach (Person p in pl)
      {
        dt.Rows.Add(p.staffing_id, p.disaster_name.Trim(), p.work_date,
          p.staffing_hours, p.employee_name.Trim(), p.employee_id,
          p.staffing_start_date, p.staffing_end_date, p.payroll_code,
          p.pay_period_hours, p.comment.Trim(), p.unit.Trim(),
          p.work_type.Trim(), p.unit_name.Trim(), p.payroll_rule,
          p.pay_rate);
      }
      string query = @"        

        SET NOCOUNT, XACT_ABORT ON;
        USE DisasterData;

        MERGE DisasterData.dbo.PersonTelestaff WITH (HOLDLOCK) AS PTS

        USING @Person AS P ON PTS.staffing_id = P.staffing_id

        WHEN MATCHED THEN
          
          UPDATE 
          SET
            disaster_name=P.disaster_name,
            work_date=P.work_date,
            staffing_hours=P.staffing_hours,
            employee_name=P.employee_name,
            employee_id=P.employee_id,
            staffing_start_date=P.staffing_start_date,
            staffing_end_date=P.staffing_end_date,
            payroll_code=P.payroll_code,
            pay_period_hours=P.pay_period_hours,
            comment=P.comment,
            unit=P.unit,
            work_type=P.work_type,
            unit_name=P.unit_name,
            payroll_rule=P.payroll_rule,
            pay_rate=P.pay_rate

        WHEN NOT MATCHED BY TARGET THEN

          INSERT 
            (
              staffing_id
              ,disaster_name
              ,work_date
              ,staffing_hours
              ,employee_name
              ,employee_id
              ,staffing_start_date
              ,staffing_end_date
              ,payroll_code
              ,pay_period_hours
              ,comment
              ,unit
              ,work_type
              ,unit_name
              ,payroll_rule
              ,pay_rate
            )
          VALUES 
            (
              P.staffing_id
              ,P.disaster_name
              ,P.work_date
              ,P.staffing_hours
              ,P.employee_name
              ,P.employee_id
              ,P.staffing_start_date
              ,P.staffing_end_date
              ,P.payroll_code
              ,P.pay_period_hours
              ,P.comment
              ,P.unit
              ,P.work_type
              ,P.unit_name
              ,P.payroll_rule
              ,P.pay_rate
            )

        WHEN NOT MATCHED BY SOURCE THEN
        
          DELETE;";
      try
      {
        using (IDbConnection db = new SqlConnection(Program.GetCS(target)))
        {
          db.Execute(query, new { Person = dt.AsTableValuedParameter("PersonTelestaffData") });
        }
      }

      catch (Exception ex)
      {
        new ErrorLog(ex, query);
      }


    }

  }
}
