using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.ServiceProcess;
using System.Timers;
using System.Threading;
using System.Data;
namespace KDSIntegration
{
    public partial class Service1 : ServiceBase
    {
        System.Timers.Timer timer = new System.Timers.Timer();
        string OrderStatusID = "";
        string FryingItem = "";
        decimal result = 0;
        DateTime PreviousOrderDateTime = Convert.ToDateTime("00:00:00");
        string Date = "";
        string TimeP = "";
        string Values = "";
        string ConcatenatedString = "";
        public Service1()
        {
            InitializeComponent();
        }
        public void onDebug()
        {
            OnStart(null);
        }
        protected override void OnStart(string[] args)
        {
            WriteToFile("Service is started at " + DateTime.Now);
            timer.Elapsed += new ElapsedEventHandler(OnElapsedTime);
            timer.Interval = 5000; // Number in milisecinds i.e. 5 sec  
            timer.Enabled = true;
            FetchAndWriteData();
        }
        protected override void OnStop()
        {
            WriteToFile("Service is stopped at " + DateTime.Now);
        }
        private void OnElapsedTime(object source, ElapsedEventArgs e)
        {
            WriteToFile("Service is recall at " + DateTime.Now);
            FetchAndWriteData();
        }
        public void WriteToFile(string Message)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "\\Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\ServiceLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
            if (!File.Exists(filepath))
            {
                // Create a file to write to.   
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
        }
        public void FetchAndWriteData()
        {
            try
            {

                string value = "";
                DateTime NextTime = Convert.ToDateTime("00:00:00");

                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                conn.Open();
                string query = @"Select Value from Configuration where ConfigurationID=1";

                SqlCommand cmd = new SqlCommand(query, conn);
                SqlDataReader dr = cmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        value = dr["Value"].ToString();
                    }
                }
                dr.Close();
                conn.Close();

                SqlConnection conn2 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                conn2.Open();
                string query2 = @"Select Value from Configuration where ConfigurationID=6";

                SqlCommand cmd2 = new SqlCommand(query2, conn2);
                SqlDataReader dr2 = cmd2.ExecuteReader();

                if (dr2.HasRows)
                {
                    while (dr2.Read())
                    {
                        NextTime = DateTime.Parse(dr2["Value"].ToString(), System.Globalization.CultureInfo.InvariantCulture);
                    }
                }
                dr2.Close();
                conn2.Close();

                DateTime TimeNow = DateTime.Now;

                if (value == "1")
                {
                    SqlConnection connup = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                    connup.Open();

                    string UPQuery = @"Delete from Item
                                       Delete from BOM";

                    SqlCommand cmdup = new SqlCommand(UPQuery, connup);
                    SqlDataReader drup = cmdup.ExecuteReader();

                    drup.Close();
                    connup.Close();

                    GetItemData();
                    CreateBOMWithOutKIT();
                    CreateBOMWithKIT();
                }
                else if (TimeNow > NextTime)
                {
                    //DateTime NextUpdatedTime = NextTime.AddHours(2);
                    DateTime NextUpdatedTime = NextTime.AddMinutes(10);

                    string x = "";
                    x = NextUpdatedTime.ToString("MM-dd-yyyy HH:mm:ss", CultureInfo.InvariantCulture);

                    SqlConnection connup = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                    connup.Open();

                    string UPQuery = @"Delete from ItemForecast
                                       Update Configuration Set Value='" + x + "' where ConfigurationId=6";

                    SqlCommand cmdup = new SqlCommand(UPQuery, connup);
                    SqlDataReader drup = cmdup.ExecuteReader();

                    drup.Close();
                    connup.Close();
                    //timer.Enabled = false;
                    //GetItemForecastData();
                    //timer.Enabled = true;
                }

                ResetBlinkingOrders();
                GetOrderData();

                // bilal 
                GetFryingItem();
                //----------------


                WriteToFile("Successfully Termindated! at: " + DateTime.Now);
            }
            catch (Exception ex)
            {
                WriteToFile("Main Message: " + ex.Message);
            }

        }
        public void GetItemData()
        {
            try
            {
                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSRetail"].ConnectionString);
                conn.Open();

                string query = @"select Distinct Item.ITEMID,item.PRODUCT,ItemTranslation.NAME Name,ItemGroup.ITEMGROUPID,
                                '' Category,
                                ax.RETAILINVENTTABLE.ISFRYABLE,ax.RETAILINVENTTABLE.FriedStation,ax.RETAILINVENTTABLE.PastaStation,ax.RETAILINVENTTABLE.PizzaStation,
								RETAILKIT.PRODUCTMASTER
                                from ax.INVENTTABLE as Item 
				left outer join ax.INVENTITEMGROUPITEM as ItemGroup on ItemGroup.ITEMID = Item.ITEMID and ItemGroup.ITEMDATAAREAID = Item.DATAAREAID
                                inner join ax.ECORESPRODUCTTRANSLATION as ItemTranslation on ItemTranslation.PRODUCT = Item.PRODUCT
                                inner join ax.RETAILASSORTMENTLOOKUP as ass on ass.PRODUCTID = Item.PRODUCT
                                left outer join ax.RETAILINVENTTABLE on ax.RETAILINVENTTABLE.ITEMID = Item.ITEMID
                                left outer join ax.RETAILKIT on RETAILKIT.PRODUCTMASTER = Item.PRODUCT
                                where Item.DATAAREAID = 'CHZ'";

                SqlCommand cmd = new SqlCommand(query, conn);
                SqlDataReader dr = cmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        try
                        {
                            string ItemCode = dr["ITEMID"].ToString(); //dr.GetInt32(0);
                            string ItemName = dr["NAME"].ToString(); //dr.GetString(1);
                            string ItemCategory = dr["Category"].ToString();
                            string Fried = dr["ISFRYABLE"].ToString();
                            string PRODUCTMASTER = dr["PRODUCTMASTER"].ToString();
                            //DK
                            string StationCat = dr["FriedStation"].ToString();
                            string StationCat1 = dr["PizzaStation"].ToString();
                            string StationCat2 = dr["PastaStation"].ToString();

                            //
                            SqlConnection conn2 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                            conn2.Open();
                            //dk add station catagory
                            string Query = @"insert into Item(ItemId,ItemName,ItemCategory,IsFried,ProductMaster,FriedStation,PizzaStation,PastaStation) 
                                        values('" + ItemCode + "','" + ItemName + "','" + ItemCategory + "','" + Fried + "','" + PRODUCTMASTER + "','" + StationCat + "','" + StationCat1 + "','" + StationCat2 + "');";

                            SqlCommand cmd2 = new SqlCommand(Query, conn2);
                            SqlDataReader dr2 = cmd2.ExecuteReader();

                            dr2.Close();
                            conn2.Close();
                            WriteToFile("Item Successfully Created: " + ItemCode);
                        }
                        catch (Exception ex)
                        {
                            WriteToFile(ex.Message);
                        }
                    }
                }
                else
                {
                    this.WriteToFile("No new data found in Retail DB");
                }

                dr.Close();
                conn.Close();

                SqlConnection connup = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                connup.Open();

                string UPQuery = @"Update Configuration Set Value='0' Where ConfigurationID='1'";

                SqlCommand cmdup = new SqlCommand(UPQuery, connup);
                SqlDataReader drup = cmdup.ExecuteReader();

                drup.Close();
                connup.Close();
            }
            catch (Exception ex)
            {
                WriteToFile(ex.Message);
            }
        }
        public void CreateBOMWithOutKIT()
        {
            try
            {
                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSRetail"].ConnectionString);
                conn.Open();
                string query = @"select Item.ITEMID, BOM.ITEMID FryingItem , SUM(BOM.BOMQTY) BOMQTY, Bom.UNITID, BOM.BOMID,
								 RETAILINVENTTABLE.ISFRYABLE,RETAILINVENTTABLE.FriedStation,RETAILINVENTTABLE.PastaStation,RETAILINVENTTABLE.PizzaStation,
								 INVENTDIM.CONFIGID
                                 from ax.INVENTTABLE as Item 
                                 left outer join ext.BOMVERSION on BOMVERSION.ITEMID = Item.ITEMID
                                 left outer join ext.BOM on bom.BOMID = BOMVERSION.BOMID
                                 left outer join ax.RETAILINVENTTABLE on RETAILINVENTTABLE.ITEMID = BOM.ITEMID
                                 left outer join ax.INVENTDIM on INVENTDIM.INVENTDIMID = BOMVERSION.INVENTDIMID
                                 where Item.DATAAREAID = 'CHZ' 
                                 and BOMVERSION.ACTIVE = 1 and ISFRYABLE=1
                                 Group By Item.ITEMID, BOM.ITEMID, Bom.UNITID, BOM.BOMID, RETAILINVENTTABLE.ISFRYABLE,RETAILINVENTTABLE.FriedStation,
								 RETAILINVENTTABLE.PastaStation,RETAILINVENTTABLE.PizzaStation,INVENTDIM.CONFIGID
								 order by 2";

                SqlCommand cmd = new SqlCommand(query, conn);
                SqlDataReader dr = cmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        string ItemId = dr["ITEMID"].ToString(); //dr.GetInt32(0);
                        string BOMId = dr["BOMID"].ToString(); //dr.GetString(1);
                        string Qty = dr["BOMQTY"].ToString();
                        string Unit = dr["UNITID"].ToString();
                        string Config = dr["CONFIGID"].ToString();
                        string FryingItem = dr["FryingItem"].ToString();

                        SqlConnection conn2 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                        conn2.Open();

                        string Query = @"insert into BOM(ItemId,BOMId,Quantity,Unit,CONFIGID,FryingItem) 
                                        values('" + ItemId + "','" + BOMId + "','" + Qty + "','" + Unit + "','" + Config + "','" + FryingItem + "');";

                        SqlCommand cmd2 = new SqlCommand(Query, conn2);
                        SqlDataReader dr2 = cmd2.ExecuteReader();

                        dr2.Close();
                        conn2.Close();
                        WriteToFile("BOM Item Successfully Created: " + ItemId);

                    }
                }
                else
                {
                    this.WriteToFile("No new data found in Retail DB");
                }

                dr.Close();
                conn.Close();

            }
            catch (Exception ex)
            {
                WriteToFile(ex.Message);
            }

        }

        // bilal --------------------------------------------------------------
        public void UpdTotalSold_OnHand(string itemid, decimal Qty)
        {
            string updpizzastationquery;
            string cs = ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString;
            SqlConnection con = new SqlConnection(cs);
            
            SqlCommand PizzaUpdCmd;
            string BomTableQuery = "select ItemId from RetailChannelDatabase.ext.BOM where BOMID=@bomid";
            SqlDataAdapter sda2 = new SqlDataAdapter(BomTableQuery, con);
            sda2.SelectCommand.Parameters.AddWithValue("@bomid", itemid);
            DataSet ds2 = new DataSet();
            sda2.Fill(ds2);
            //DataTable dt = new DataTable();
            if (ds2.Tables[0].Rows.Count > 0)
            {
                foreach ( DataRow dr2 in ds2.Tables[0].Rows)
                {
                    string itmid = dr2[0].ToString();
                    Console.WriteLine(itmid);
                    // Fetch Data PizzaStation Table 
                    string FetchPizzaQuery = "select * from PizzaStation_Tbl where itemid = @itemid";
                    SqlDataAdapter sda1  = new SqlDataAdapter(FetchPizzaQuery, con);
                    sda1.SelectCommand.Parameters.AddWithValue("@itemid", dr2[0].ToString() );
                    DataSet ds1 = new DataSet();
                    sda1.Fill(ds1);
                    if (ds1.Tables[0].Rows.Count > 0)
                    {
                        foreach (DataRow dr1 in ds1.Tables[0].Rows)
                        {
                            if (dr1[1].ToString() != "" && dr1[3].ToString() != "")
                            {
                                updpizzastationquery = "update PizzaStation_Tbl set TotalSold = TotalSold + @qty , InHand = InHand - @Qty where itemid = @itemid";
                            }
                            else if (dr1[1].ToString() != "")
                            {
                                updpizzastationquery = "update PizzaStation_Tbl set InHand = InHand - @qty , TotalSold = @Qty where itemid = @itemid";
                            }
                            else if (dr1[3].ToString() != "")
                            {
                                updpizzastationquery = "update PizzaStation_Tbl set TotalSold = TotalSold + @qty , InHand = -@qty where itemid = @itemid";
                            }
                            else
                            {
                                updpizzastationquery = "update PizzaStation_Tbl set InHand = -@qty, TotalSold = @qty  where itemid = @itemid";
                            }
                            PizzaUpdCmd = new SqlCommand(updpizzastationquery, con);
                            PizzaUpdCmd.Parameters.AddWithValue("@ItemId", dr1["ItemId"].ToString());
                            PizzaUpdCmd.Parameters.AddWithValue("@Qty", Qty);
                            con.Open();
                                PizzaUpdCmd.ExecuteNonQuery();
                            con.Close();
                        }
                    }
                }
            }
            con.Close();
        }
        public void GetFryingItem()
        {
            string cs = ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString;

            try
            {
                SqlConnection con = new SqlConnection(cs);
                string query = "select * from Item where IsFried = 1 ";
                SqlDataAdapter sda = new SqlDataAdapter(query, con);
                DataTable dt = new DataTable();
                sda.Fill(dt);
                if (dt.Rows.Count > 0)
                {
                    foreach (DataRow dr in dt.Rows)
                    {
                        GetFryingItemAndFinishGood(dr["ItemId"].ToString());
                    }
                }
            }
            catch (Exception Ex)
            {

            }
        }
        public void GetFryingItemAndFinishGood(string ItemId)
        {
            string FryingItem = "";
            string ItemIds = "";
            string BomId = "";
            string UnitId = "";
            string BomQty = "";
            string ConfigId = "";
            string cs = ConfigurationManager.ConnectionStrings["CSRetail"].ConnectionString;
            try
            {
                SqlConnection con = new SqlConnection(cs);
                string query = "select * from ext.Bom where itemid = @itemid";
                SqlDataAdapter sda = new SqlDataAdapter(query, con);
                sda.SelectCommand.Parameters.AddWithValue("@ItemId", ItemId);
                DataTable dt = new DataTable();
                sda.Fill(dt);
                if (dt.Rows.Count > 0)
                {
                    foreach (DataRow dr in dt.Rows)
                    {
                        //insert into Bom Table 
                        FryingItem = dr["ITEMID"].ToString();
                        BomId = dr["BOMID"].ToString();
                        BomQty = dr["BOMQTY"].ToString();
                        UnitId = dr["UNITID"].ToString();
                        ConfigId = "";  //dr["CONFIGID"].ToString();
                        ItemId = dr["BOMID"].ToString();
                        InsertDataIntoBom(FryingItem, BomId, BomQty, UnitId, ConfigId, ItemId);
                    }
                }
                con.Close();
            }
            catch (Exception Ex)
            {

            }
        }
        public void InsertDataIntoBom(string FryingItem, string BomId, string BomQty, string UnitId, string configid, string ItemID)
        {
            int checkCount = 0;
            string cs = ConfigurationManager.ConnectionStrings ["CSKDS"].ConnectionString;
            try
            {
                checkCount = CheckExistingItems(FryingItem, ItemID);
                if(checkCount == 0)
                {
                    SqlConnection con = new SqlConnection(cs);
                    string InsertQuery = @"Insert Into Bom (ItemId,Unit,BomId,Quantity,CONFIGID,FryingItem)
                                Values(@itemid,@unitid,@BomId,@BomQty,@ConfigId,@FryingItem)";

                    SqlCommand cmd = new SqlCommand(InsertQuery, con);
                    cmd.Parameters.AddWithValue("@itemid", ItemID);
                    cmd.Parameters.AddWithValue("@unitid", UnitId);
                    cmd.Parameters.AddWithValue("@BomId", BomId);
                    cmd.Parameters.AddWithValue("@BomQty", BomQty);
                    cmd.Parameters.AddWithValue("@ConfigId", configid);
                    cmd.Parameters.AddWithValue("@FryingItem", FryingItem);

                    con.Open();
                    int i = cmd.ExecuteNonQuery();
                    if (i > 0)
                    {
                        WriteToFile("Data Inserted SuccessFully");
                    }
                    else
                    {
                        WriteToFile("Data Not Inserted");
                    }
                    con.Close();
                }
            }
            catch (Exception ex)
            {
                WriteToFile(ex.Message);
            }
        }
        public int CheckExistingItems(string FryingItem,string ItemId)
        {
            int rowCount = 0;
            string cs = ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString;
            try
            {
                SqlConnection con = new SqlConnection(cs);
                string CheckExist = "select * from BOM where FryingItem = @fryingitem and BomId = @itemid";
                SqlDataAdapter sda = new SqlDataAdapter(CheckExist, con);
                sda.SelectCommand.Parameters.AddWithValue("@fryingitem", FryingItem);
                sda.SelectCommand.Parameters.AddWithValue("@itemid", ItemId);
                DataTable dt = new DataTable();
                sda.Fill(dt);
                if(dt.Rows.Count > 0)
                {
                    rowCount = dt.Rows.Count;
                }

            }
            catch(Exception ex)
            {
                WriteToFile(ex.Message);
            }
            return rowCount;
        }


        // -------------------------------------------------------------------- bilal section End 
        //public void PizzaStationDataNull()
        //{

        //}
        public void CreateBOMWithKIT()
        {
            try
            {
                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSRetail"].ConnectionString);
                conn.Open();
                string query = @"select  Item,ConfigID,FryingItem,UNITID,BOMID,ISFRYABLE,FriedStation,PastaStation,PizzaStation,sum(TotalFryingItemQ) TotalFryingItemQ from
                (select IT2.ITEMID Item, ECORESCONFIGURATION.NAME ConfigID, BOM.ITEMID FryingItem,  UNITID,BOM.BOMID,
                RETAILINVENTTABLE.ISFRYABLE,RETAILINVENTTABLE.FriedStation,RETAILINVENTTABLE.PastaStation,RETAILINVENTTABLE.PizzaStation, 
				RETAILKITCOMPONENT.QUANTITY * BOMQTY TotalFryingItemQ
                from ax.RETAILKITCOMPONENT
                join ax.ECORESPRODUCT on ECORESPRODUCT.RECID = RETAILKITCOMPONENT.COMPONENT
                join ax.ECORESDISTINCTPRODUCTVARIANT on ECORESDISTINCTPRODUCTVARIANT.RECID = ECORESPRODUCT.RECID
                join ax.ECORESPRODUCT P1 on P1.RECID = ECORESDISTINCTPRODUCTVARIANT.PRODUCTMASTER--ECORESPRODUCT.RECID
                join ax.ECORESPRODUCTVARIANTDIMENSIONVALUE on ECORESPRODUCTVARIANTDIMENSIONVALUE.DISTINCTPRODUCTVARIANT = ECORESDISTINCTPRODUCTVARIANT.RECID--P1.RECID -- ECORESPRODUCT.RECID
                join ax.ECORESPRODUCTVARIANTCONFIGURATION on ECORESPRODUCTVARIANTCONFIGURATION.RECID = ECORESPRODUCTVARIANTDIMENSIONVALUE.RECID
                join ax.ECORESCONFIGURATION on ECORESCONFIGURATION.RECID = ECORESPRODUCTVARIANTCONFIGURATION.CONFIGURATION
                left outer join ax.RETAILKIT on RETAILKIT.RECID = ax.RETAILKITCOMPONENT.KIT
                join ax.INVENTTABLE IT on IT.PRODUCT = P1.RECID
                join ext.BOMVERSION on BOMVERSION.ITEMID = IT.ITEMID
                join ext.BOM on BOM.BOMID = BOMVERSION.BOMID
                join ax.INVENTDIM on INVENTDIM.INVENTDIMID = BOMVERSION.INVENTDIMID and INVENTDIM.CONFIGID = ECORESCONFIGURATION.NAME
                join ax.INVENTTABLE IT2 on IT2.PRODUCT = RETAILKIT.PRODUCTMASTER
                join ax.RETAILINVENTTABLE on RETAILINVENTTABLE.ITEMID = BOM.ITEMID
                where  BOMVERSION.ACTIVE = 1 
                and ISFRYABLE = 1 ) itemBOM Group by Item,ConfigID,FryingItem,UNITID,BOMID,ISFRYABLE,FriedStation,PastaStation,PizzaStation";

                SqlCommand cmd = new SqlCommand(query, conn);
                SqlDataReader dr = cmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        string ItemId = dr["Item"].ToString();
                        string BOMId = dr["BOMID"].ToString();
                        string Qty = dr["TotalFryingItemQ"].ToString();
                        string Unit = dr["UNITID"].ToString();
                        string Config = dr["ConfigID"].ToString();
                        string FryingItem = dr["FryingItem"].ToString();

                        SqlConnection conn2 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                        conn2.Open();

                        string Query = @"insert into BOM(ItemId,BOMId,Quantity,Unit,CONFIGID,FryingItem) 
                                        values('" + ItemId + "','" + BOMId + "','" + Qty + "','" + Unit + "','" + Config + "','" + FryingItem + "');";

                        SqlCommand cmd2 = new SqlCommand(Query, conn2);
                        SqlDataReader dr2 = cmd2.ExecuteReader();

                        dr2.Close();
                        conn2.Close();
                        WriteToFile("BOM Item Successfully Created: " + ItemId);
                    }
                }
                else
                {
                    this.WriteToFile("No new data found in Retail DB");
                }

                dr.Close();
                conn.Close();

            }
            catch (Exception ex)
            {
                WriteToFile(ex.Message);
            }
        }
        public void GetOrderData()
        {
            try
            {
                SqlConnection connPOI = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                connPOI.Open();
                string queryPOI = @"Select Value from Configuration Where ConfigurationID=2";

                SqlCommand cmdPOI = new SqlCommand(queryPOI, connPOI);
                SqlDataReader drPOI = cmdPOI.ExecuteReader();


                if (drPOI.HasRows)
                {
                    while (drPOI.Read())
                    {
                        PreviousOrderDateTime = DateTime.Parse(drPOI["Value"].ToString(), System.Globalization.CultureInfo.InvariantCulture);

                        Date = PreviousOrderDateTime.ToString("yyyy-MM-dd");
                        TimeP = PreviousOrderDateTime.ToString("HH:mm:ss");
                        //TimeP = PreviousOrderDateTime.TimeOfDay.TotalSeconds.ToString();

                        ConcatenatedString = Date + " " + TimeP;
                    }
                }

                drPOI.Close();
                connPOI.Close();


                SqlConnection connPOI1 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                connPOI1.Open();
                string queryPOI1 = @"Select Value from Configuration Where ConfigurationID=5";

                SqlCommand cmdPOI1 = new SqlCommand(queryPOI1, connPOI1);
                SqlDataReader drPOI1 = cmdPOI1.ExecuteReader();

                if (drPOI1.HasRows)
                {
                    while (drPOI1.Read())
                    {
                        Values = drPOI1["Value"].ToString();
                    }
                }
                string configvalues = "";
                char[] spearator1 = { ',' };
                String[] stationslist1 = Values.Split(spearator1);
                foreach (string list in stationslist1)
                {
                    if (list == "DINE IN")
                    {
                        if (configvalues == "")
                            configvalues += "'01'";
                        else { configvalues += ",'01'"; }
                    }
                    else if (list == "TAKE AWAY")
                    {
                        if (configvalues == "")
                            configvalues += "'02'";
                        else { configvalues += ",'02'"; }
                    }
                    else if (list == "DELIVERY")
                    {
                        if (configvalues == "")
                            configvalues += "'03'";
                        else { configvalues += ",'03'"; }
                    }
                    else if (list == "DRIVE THRU")
                    {
                        if (configvalues == "")
                            configvalues += "'04'";
                        else { configvalues += ",'04'"; }
                    }
                    else if (list == "EMPLOYEE MEAL")
                    {
                        if (configvalues == "")
                            configvalues += "'05'";
                        else { configvalues += ",'05'"; }
                    }
                }

                drPOI1.Close();
                connPOI1.Close();

                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSRetail"].ConnectionString);
                conn.Open();
                //dk add query line 
                string query = @" Select *,( select top 1 DTral.name
                                from  ax.ECORESCONFIGURATION as conf
                               Inner Join ax.ECORESPRODUCTMASTERCONFIGURATION as PMConf on PMConf.CONFIGURATION = conf.RECID
                               inner join ax.ECORESPRODUCTMASTERDIMENSIONVALUE as EcpDm on EcpDm.RECID = PMConf.RECID
                               Inner Join ax.ECORESPRODUCTMASTERDIMVALUETRANSLATION as DTral on DTral.ProductMasterDimensionValue = EcpDm.RECID
                               where conf.NAME = Orders.ConfigID ) ConfigIDNam,
                               (SELECT  STRING_AGG(T.DESCRIPTION ,',')as Information   from ax.RETAILTRANSACTIONINFOCODETRANS  as infocode
                               join ax.RETAILINFOCODETABLE M on M.INFOCODEID = infocode.INFOCODEID
							   join ax.RETAILINFOCODETRANSLATION T on T.INFOCODE = M.RECID
                               where infocode.TRANSACTIONID =Orders.TRANSACTIONID
                               and infocode.PARENTLINENUM =Orders.LINENUM
							    and INFOCODE.INFORMATION = 'Yes'
                               group by parentlinenum
                               ) as Information
                               from(select Line.TRANSACTIONID, Line.TRANSDATE, Line.TRANSACTIONSTATUS, TransTable.ENTRYSTATUS,
                               RIGHT('0' + CAST(Line.TRANSTIME / 3600 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST((Line.TRANSTIME / 60) % 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(Line.TRANSTIME % 60 AS VARCHAR), 2) as TRANSTIME,
                               CONCAT(Line.TRANSDATE, ' ', Line.TRANSTIME) as OrderDateTime,
                               TransTable.SUSPENDEDTRANSACTIONID, TransTable.HDSOrderID, TransTable.DESCRIPTION, TransTable.TYPE as TransactionType,
                               Line.ITEMID, Line.LINENUM, Line.RECEIPTID, Line.RETURNTRANSACTIONID, TransTable.CREATEDDATETIME,
                               Line.QTY, Line.TERMINALID, Line.STORE, ax.RETAILINVENTTABLE.FriedStation, ax.RETAILINVENTTABLE.PastaStation, ax.RETAILINVENTTABLE.PizzaStation,
                               Translation.DESCRIPTION as 'ITEM DESCRIPTION', Translation.NAME as 'ITEM NAME', ItemDim.CONFIGID ItemConfig, Line.COMMENT,
                               CatTrans.FRIENDLYNAME as 'CATEGORY', isnull((select top 1 ECORESCONFIGURATION.NAME
                               from ax.RETAILKIT
                               join ax.RETAILKITCOMPONENT on RETAILKIT.RECID = RETAILKITCOMPONENT.KIT
                               join ax.ECORESPRODUCT on ECORESPRODUCT.RECID = RETAILKITCOMPONENT.COMPONENT
                               join ax.ECORESDISTINCTPRODUCTVARIANT on ECORESDISTINCTPRODUCTVARIANT.RECID = ECORESPRODUCT.RECID
                               join ax.ECORESPRODUCT P1 on P1.RECID = ECORESDISTINCTPRODUCTVARIANT.PRODUCTMASTER
                               join ax.ECORESPRODUCTVARIANTDIMENSIONVALUE on ECORESPRODUCTVARIANTDIMENSIONVALUE.DISTINCTPRODUCTVARIANT = ECORESDISTINCTPRODUCTVARIANT.RECID
                               join ax.ECORESPRODUCTVARIANTCONFIGURATION on ECORESPRODUCTVARIANTCONFIGURATION.RECID = ECORESPRODUCTVARIANTDIMENSIONVALUE.RECID
                               join ax.ECORESCONFIGURATION on ECORESCONFIGURATION.RECID = ECORESPRODUCTVARIANTCONFIGURATION.CONFIGURATION
                               where RETAILKIT.PRODUCTMASTER = Item.PRODUCT), ItemDim.CONFIGID) ConfigID
                               from ax.RETAILTRANSACTIONSALESTRANS as Line
                               inner join ax.RETAILTRANSACTIONTABLE TransTable on TransTable.TRANSACTIONID = Line.TRANSACTIONID
                               inner join ax.INVENTDIMCOMBINATION AS DimCombination on DimCombination.RETAILVARIANTID = Line.VARIANTID and DimCombination.ITEMID = Line.ITEMID
                               inner Join ax.INVENTDIM as ItemDim on ItemDim.INVENTDIMID = DimCombination.INVENTDIMID
                               inner Join ax.INVENTTABLE as Item  on Item.ITEMID = Line.ITEMID
                               inner join ax.RETAILINVENTTABLE on ax.RETAILINVENTTABLE.ITEMID = Line.ITEMID
                               inner join ax.ECORESPRODUCT as Product on Product.RECID = Item.PRODUCT
                               inner Join ax.ECORESPRODUCTTRANSLATION as Translation on Translation.PRODUCT = Item.PRODUCT
                               inner join ax.ECORESPRODUCTCATEGORY cat on item.product=cat.product
                               inner join ax.ECORESCATEGORYTRANSLATION as CatTrans on CatTrans.CATEGORY = cat.CATEGORY
                               where Line.DATAAREAID = 'CHZ' and ConfigID!= '07' and Line.TRANSACTIONSTATUS != 1  
                              and CatTrans.FRIENDLYNAME != 'EXTRA TOPPING'
                                and Line.RECEIPTID!=''
                               and(TransTable.SUSPENDEDTRANSACTIONID = '' OR(TransTable.SUSPENDEDTRANSACTIONID != '')))Orders
                                where (TRANSDATE = '" + Date + " ' and TRANSTIME >  '" + TimeP + "') OR TRANSDATE > '" + Date + "' Order by TRANSDATE, TRANSTIME asc";

                SqlCommand cmd = new SqlCommand(query, conn);
                SqlDataReader dr = cmd.ExecuteReader();
                string x = string.Empty;
                string HDSOID = string.Empty;

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        try
                        {


                            string OrderStatus = "";

                            string OrderStatusID = "";
                            string OrderState = "";
                            string FriedStatus = "";
                            //string OrderID = dr["RECEIPTID"].ToString();
                            string OrderID = dr["TRANSACTIONID"].ToString();
                            //string OrderID = dr["SUSPENDEDTRANSACTIONID"].ToString();
                            string OrderNo = "";
                            string OrderDate = dr["TRANSDATE"].ToString();
                            string OrderTime = dr["TRANSTIME"].ToString();
                            TimeSpan Time = TimeSpan.Parse(OrderTime);
                            DateTime date = Convert.ToDateTime(OrderDate);
                            x = date.ToString("MM-dd-yyyy", CultureInfo.InvariantCulture);
                            x = x + " " + OrderTime;
                            string POSID = dr["TERMINALID"].ToString();
                            string ItemID = dr["ITEMID"].ToString();
                            string ItemName = dr["ITEM NAME"].ToString();
                            decimal Quantity = Convert.ToDecimal(dr["QTY"].ToString()) * -1;
                            string Description = dr["ITEM DESCRIPTION"].ToString();
                            string OrderType = dr["ConfigIDNam"].ToString();
                            string OrderTypeID = dr["ConfigID"].ToString();
                            string StoreID = dr["STORE"].ToString();
                            string Category = dr["CATEGORY"].ToString();
                            string TransactionID = dr["TRANSACTIONID"].ToString();
                            string ReturnTransactionID = dr["RETURNTRANSACTIONID"].ToString();
                            decimal LINENUM1 = Convert.ToDecimal(dr["LINENUM"].ToString());
                            string LINENUM = decimal.Round(LINENUM1, 2, MidpointRounding.AwayFromZero).ToString();
                            HDSOID = dr["HDSOrderId"].ToString();
                            string TransctionT = dr["TransactionType"].ToString();
                            string OrderSource = dr["DESCRIPTION"].ToString();
                            string SuspendedTrans = dr["SUSPENDEDTRANSACTIONID"].ToString();
                            string EntryStatus = dr["ENTRYSTATUS"].ToString();
                            //dk add colums
                            string StationCat = dr["FriedStation"].ToString();
                            string StationCat1 = dr["PizzaStation"].ToString();
                            string StationCat2 = dr["PastaStation"].ToString();
                            string Comments = dr["COMMENT"].ToString();
                            string Information = dr["INFORMATION"].ToString();

                            //
                            ConcatenatedString = OrderDate + " " + OrderTime;
                            string OrStatusId = "";

                            if (SuspendedTrans != "")
                                HDSOID = SuspendedTrans;
                            else if (HDSOID == "" && SuspendedTrans == "")
                            {
                                HDSOID = CheckSuspendedOrderPOS(TransactionID);
                            }

                            if (HDSOID != "")
                            {
                                OrderID = GetOrderID(HDSOID);
                                if (OrderID != "")
                                {
                                    SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                    conn4.Open();

                                    string ItemUpdateQuery = @"Update Orders Set OrderId='" + OrderID + @"' Where HDSOrderId='" + HDSOID + @"' and OrderSource=''";

                                    SqlCommand itemupdatecmd = new SqlCommand(ItemUpdateQuery, conn4);
                                    SqlDataReader itemupdatedr = itemupdatecmd.ExecuteReader();
                                    itemupdatedr.Close();
                                    conn4.Close();
                                }
                                else
                                {
                                    OrderID = HDSOID;
                                }
                            }

                            //string Length = OrderID.Substring(Math.Max(0, OrderID.Length - 4));
                            //OrderID = Length;

                            SqlConnection con3 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                            con3.Open();

                            string Query3 = @"Select * from Orders where HDSOrderId = '" + HDSOID + "' and ItemId ='" + ItemID + "'";

                            SqlCommand cmd3 = new SqlCommand(Query3, con3);
                            SqlDataReader dr3 = cmd3.ExecuteReader();

                            if (dr3.HasRows && HDSOID != "")
                            {
                                while (dr3.Read())
                                {
                                    if (TransctionT == "36")
                                    {
                                        //string OID = dr3["OrderId"].ToString();
                                        //OrStatusId = dr3["OrderStatusId"].ToString();
                                        //if (Convert.ToInt32(OrStatusId) >= 3)
                                        //{
                                        //    //GetduplicateOrderLine(OrderID, ItemID, LINENUM);
                                        //    break;
                                        //}
                                        //else
                                        //{
                                        //    DeleteSuspendedOrder(HDSOID, OID);
                                        //    break;
                                        //}
                                        break;
                                    }
                                }
                            }

                            con3.Close();
                            dr3.Close();

                            decimal Quty;
                            if (ReturnTransactionID != "") { Quty = -(Quantity); }
                            else { Quty = Quantity; }

                            SqlConnection con = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                            con.Open();

                            string Query1 = @"Select * from Orders where HDSOrderId = '" + HDSOID + "' and ITEMID='" + ItemID + "'and Quantity='" + Quty + "'and Createdon !='" + x + "'";

                            SqlCommand cmd1 = new SqlCommand(Query1, con);
                            SqlDataReader dr1 = cmd1.ExecuteReader();

                            if (dr1.HasRows && HDSOID != "")
                            {
                                while (dr1.Read())
                                {
                                    if (TransctionT == "2" && EntryStatus == "0" && ReturnTransactionID == "")
                                    {
                                        UpdateHDSOrder(TransctionT, HDSOID, ItemID);
                                    }
                                    else if (TransctionT == "2" && EntryStatus == "1")
                                    {
                                        string OrderItemID = dr1["ITEMID"].ToString();
                                        UpdateVoidSuspendedOrder(HDSOID, OrderItemID, OrderTypeID, Quantity);
                                    }
                                    else if (TransctionT == "2" && ReturnTransactionID != "")
                                    {
                                        string OrderItemID = dr1["ITEMID"].ToString();
                                        UpdateVoidSuspendedOrder(HDSOID, OrderItemID, OrderTypeID, Quantity);
                                    }
                                }
                                dr1.Close();
                                con.Close();
                            }
                            else
                            {
                                if (OrderType != "WASTAGE")
                                {
                                    SqlConnection connO = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                    connO.Open();

                                    string QueryO = @"Select ItemId,Unit,BOMId,SUM(Quantity) Quantity,FryingItem from BOM where ItemId='" + ItemID + @"'
                                          group by ItemId,Unit,BOMId,Quantity,FryingItem";

                                    SqlCommand cmdO = new SqlCommand(QueryO, connO);
                                    SqlDataReader drO = cmdO.ExecuteReader();

                                    if (drO.HasRows)
                                    {
                                        while (drO.Read())
                                        {
                                            decimal BQty = Convert.ToDecimal(drO["Quantity"].ToString());
                                            result = BQty * Quantity;

                                            FryingItem = drO["FryingItem"].ToString();
                                            decimal OHQ;

                                            SqlConnection conn3 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                            conn3.Open();

                                            string ItemQuery = @"Select * from Item where ItemId='" + FryingItem + @"'";

                                            SqlCommand itemcmd = new SqlCommand(ItemQuery, conn3);
                                            SqlDataReader itemdr = itemcmd.ExecuteReader();

                                            if (itemdr.HasRows)
                                            {
                                                while (itemdr.Read())
                                                {
                                                    OHQ = Convert.ToDecimal(itemdr["OnHandQuantity"] == DBNull.Value ? "0" : itemdr["OnHandQuantity"].ToString());

                                                    if (OHQ >= result)
                                                    {


                                                        OrderStatus = "Preparation";
                                                        OrderStatusID = "1";
                                                        OrderState = "Preparing";
                                                        FriedStatus = "1";

                                                        SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                                        conn4.Open();
                                                        DateTime TransDate = DateTime.Now;

                                                        string Translog = @"insert into [dbo].[ItemTransLog](ItemID,TransDate,Quantity,OrderID,LineNum,FriedStatus) 
                                                        values('" + FryingItem + "',GETDATE(),'" + result + "','" + OrderID + "','" + LINENUM + "','" + FriedStatus + "');";

                                                        SqlCommand translogcmd = new SqlCommand(Translog, conn4);
                                                        SqlDataReader translogdr = translogcmd.ExecuteReader();

                                                        translogdr.Close();
                                                        conn4.Close();
                                                    }

                                                    else
                                                    {
                                                        OrderStatus = "Frying";
                                                        OrderStatusID = "0";
                                                        OrderState = "Preparing";
                                                        FriedStatus = "0";

                                                        SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                                        conn4.Open();
                                                        DateTime TransDate = DateTime.Now;

                                                        string Translog = @"insert into [dbo].[ItemTransLog](ItemID,TransDate,Quantity,OrderID,LineNum,FriedStatus) 
                                                        values('" + FryingItem + "',GETDATE(),'" + result + "','" + OrderID + "','" + LINENUM + "','" + FriedStatus + "');";

                                                        SqlCommand translogcmd = new SqlCommand(Translog, conn4);
                                                        SqlDataReader translogdr = translogcmd.ExecuteReader();

                                                        translogdr.Close();
                                                        conn4.Close();

                                                        break;
                                                    }
                                                }
                                            }
                                            itemdr.Close();
                                            conn3.Close();
                                        }
                                    }

                                    if (OrderStatus == "")
                                    {
                                        OrderStatus = "Preparation";
                                        OrderStatusID = "1";
                                        OrderState = "Preparing";
                                        FriedStatus = "1";

                                        InsertOrder(OrderID, OrderNo, x, OrderType, OrderTypeID, POSID, ItemID, ItemName, Quantity, Description, Category, TransactionID, StoreID, OrderStatus, OrderStatusID, OrderState, ReturnTransactionID, LINENUM, FriedStatus, HDSOID, TransctionT, OrderSource, SuspendedTrans, StationCat, StationCat1, StationCat2, Comments, Information);
                                    }
                                    else if (OrderStatus == "Preparation")
                                    {
                                        OrderStatus = "Preparation";
                                        OrderStatusID = "1";
                                        OrderState = "Preparing";
                                        FriedStatus = "1";

                                        InsertOrder(OrderID, OrderNo, x, OrderType, OrderTypeID, POSID, ItemID, ItemName, Quantity, Description, Category, TransactionID, StoreID, OrderStatus, OrderStatusID, OrderState, ReturnTransactionID, LINENUM, FriedStatus, HDSOID, TransctionT, OrderSource, SuspendedTrans, StationCat, StationCat1, StationCat2, Comments, Information);
                                    }
                                    else
                                    {
                                        OrderStatus = "Frying";
                                        OrderStatusID = "0";
                                        OrderState = "Preparing";

                                        InsertOrder(OrderID, OrderNo, x, OrderType, OrderTypeID, POSID, ItemID, ItemName, Quantity, Description, Category, TransactionID, StoreID, OrderStatus, OrStatusId, OrderState, ReturnTransactionID, LINENUM, FriedStatus, HDSOID, TransctionT, OrderSource, SuspendedTrans, StationCat, StationCat1, StationCat2, Comments, Information);
                                    }

                                    drO.Close();
                                    connO.Close();
                                }
                                else
                                {
                                    // if Order type is Wastage
                                    OrderStatus = "Delivered";
                                    OrderStatusID = "5";
                                    OrderState = "Fulfilled";
                                    FriedStatus = "1";

                                    SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                    conn4.Open();
                                    DateTime TransDate = DateTime.Now;

                                    string Translog = @"insert into [dbo].[ItemTransLog](ItemID,TransDate,Quantity,OrderID,LineNum,FriedStatus) 
                                                values('" + FryingItem + "',GETDATE(),'" + result + "','" + OrderID + "','" + LINENUM + "','" + FriedStatus + "');";

                                    SqlCommand translogcmd = new SqlCommand(Translog, conn4);
                                    SqlDataReader translogdr = translogcmd.ExecuteReader();

                                    translogdr.Close();
                                    conn4.Close();

                                    InsertOrder(OrderID, OrderNo, x, OrderType, OrderTypeID, POSID, ItemID, ItemName, Quantity, Description, Category, TransactionID, StoreID, OrderStatus, OrderStatusID, OrderState, ReturnTransactionID, LINENUM, FriedStatus, HDSOID, TransctionT, OrderSource, SuspendedTrans, StationCat, StationCat1, StationCat2, Comments, Information);
                                }
                            }





                        }
                        catch (Exception ex)
                        {
                            WriteToFile(ex.Message);
                        }
                    }

                    // UpdateOrderTime(HDSOID, x);

                    SqlConnection connup = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                    connup.Open();

                    string UPQuery = @"Update Configuration Set Value='" + x + @"' Where ConfigurationID='2'";

                    SqlCommand cmdup = new SqlCommand(UPQuery, connup);
                    SqlDataReader drup = cmdup.ExecuteReader();


                    drup.Close();
                    connup.Close();

                }
                else
                {
                    this.WriteToFile("No data found in Retail DB");
                }

                dr.Close();
                conn.Close();
            }
            catch (Exception ex)
            {
                WriteToFile(ex.Message);
            }
        }
        public void InsertOrder(string OrderID, string OrderNo, string x, string OrderType, string OrderTypeID, string POSID, string ItemID, string ItemName, decimal Quantity, string Description, string Category, string TransactionID, string StoreID, string OrderStatus, string OrderStatusID, string OrderState, string ReturnTransactionID, string LINENUM, string FriedStatus, string HDSOID, string TransctionT, string OrderSource, string SuspendedTrans, string stationCat, string stationCat1, string stationCat2, string information, string comments)
        {
            try
            {
                string Query = "";
                if (ReturnTransactionID == "")
                {
                    if (stationCat == "0" && stationCat1 == "0" && stationCat2 == "0")
                    {
                        OrderStatus = "FOH2";
                        OrderStatusID = "3";
                        OrderState = "Preparing";
                    }

                    SqlConnection connn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                    connn.Open();
                    if (TransctionT == "2" && HDSOID == "")
                    {
                        Query = @"insert into dbo.[orders](OrderID,OrderNo,CreatedOn,OrderType,OrderTypeID,POSID,ItemID,ItemName,Quantity,Description,ItemCategory,TransactionID,StoreID,OrderStatus,OrderStatusID,OrderState,LINENUM,HDSOrderId,TransactionType,OrderSource,FriedStation,PizzaStation,PastaStation,LineDescription2,LineDescription1) 
                                        values('" + OrderID + "','" + OrderID + "','" + x + "','" + OrderType + "','" + OrderTypeID + "','" + POSID + "','" + ItemID + "','" + ItemName + "','" + Quantity + "','" + Description + "','" + Category + "','" + TransactionID + "','" + StoreID + "','" + OrderStatus + "','" + OrderStatusID + "','" + OrderState + "','" + LINENUM + "','" + HDSOID + "','" + TransctionT + "','" + OrderSource + "','" + stationCat + "','" + stationCat1 + "','" + stationCat2 + "','" + information + "','" + comments + "');";

                    }

                    else if (SuspendedTrans != "")
                    {

                        Query = @"insert into dbo.[orders](OrderID,OrderNo,CreatedOn,OrderType,OrderTypeID,POSID,ItemID,ItemName,Quantity,Description,ItemCategory,TransactionID,StoreID,OrderStatus,OrderStatusID,OrderState,LINENUM,HDSOrderId,TransactionType,OrderSource,FriedStation,PizzaStation,PastaStation,LineDescription2,LineDescription1,OnHold) 
                                        values('" + OrderID + "','" + OrderID + "','" + x + "','" + OrderType + "','" + OrderTypeID + "','" + POSID + "','" + ItemID + "','" + ItemName + "','" + Quantity + "','" + Description + "','" + Category + "','" + TransactionID + "','" + StoreID + "','" + OrderStatus + "','" + OrderStatusID + "','" + OrderState + "','" + LINENUM + "','" + SuspendedTrans + "','" + TransctionT + "','" + OrderSource + "','" + stationCat + "','" + stationCat1 + "','" + stationCat2 + "','" + information + "','" + comments + "','true');";


                    }
                    else if (TransctionT == "2" && HDSOID != "")
                    {

                        Query = @"insert into dbo.[orders](OrderID,OrderNo,CreatedOn,OrderType,OrderTypeID,POSID,ItemID,ItemName,Quantity,Description,ItemCategory,TransactionID,StoreID,OrderStatus,OrderStatusID,OrderState,LINENUM,HDSOrderId,TransactionType,OrderSource,FriedStation,PizzaStation,PastaStation,LineDescription2,LineDescription1,OnHold) 
                                        values('" + OrderID + "','" + HDSOID + "','" + x + "','" + OrderType + "','" + OrderTypeID + "','" + POSID + "','" + ItemID + "','" + ItemName + "','" + Quantity + "','" + Description + "','" + Category + "','" + TransactionID + "','" + StoreID + "','" + OrderStatus + "','" + OrderStatusID + "','" + OrderState + "','" + LINENUM + "','" + HDSOID + "','" + TransctionT + "','" + OrderSource + "','" + stationCat + "','" + stationCat1 + "','" + stationCat2 + "','" + information + "','" + comments + "','false');";

                    }
                    else
                    {
                        Query = @"insert into dbo.[orders](OrderID,OrderNo,CreatedOn,OrderType,OrderTypeID,POSID,ItemID,ItemName,Quantity,Description,ItemCategory,TransactionID,StoreID,OrderStatus,OrderStatusID,OrderState,LINENUM,HDSOrderId,TransactionType,OrderSource,FriedStation,PizzaStation,PastaStation,LineDescription2,LineDescription1,OnHold) 
                                        values('" + OrderID + "','" + HDSOID + "','" + x + "','" + OrderType + "','" + OrderTypeID + "','" + POSID + "','" + ItemID + "','" + ItemName + "','" + Quantity + "','" + Description + "','" + Category + "','" + TransactionID + "','" + StoreID + "','" + OrderStatus + "','" + OrderStatusID + "','" + OrderState + "','" + LINENUM + "','" + HDSOID + "','" + TransctionT + "','" + OrderSource + "','" + stationCat + "','" + stationCat1 + "','" + stationCat2 + "','" + information + "','" + comments + "','true');";

                    }
                    SqlCommand cmd2 = new SqlCommand(Query, connn);
                    SqlDataReader dr2 = cmd2.ExecuteReader();

                    dr2.Close();
                    connn.Close();
                    UpdTotalSold_OnHand(ItemID, Quantity);

                    //if (HDSOID != "")
                    //{
                    //    SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                    //    conn4.Open();

                    //    string ItemUpdateQuery = @"Update Orders Set CreatedOn='" + x + @"' Where HDSOrderId='" + HDSOID + @"'";

                    //    SqlCommand itemupdatecmd = new SqlCommand(ItemUpdateQuery, conn4);
                    //    SqlDataReader itemupdatedr = itemupdatecmd.ExecuteReader();
                    //    itemupdatedr.Close();
                    //    conn4.Close();
                    //}
                }

                SqlConnection conn2 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                conn2.Open();

                string Query2 = @"Select ItemId,Unit,BOMId,SUM(Quantity) Quantity,FryingItem from BOM where ItemId='" + ItemID + @"'
                                          group by ItemId,Unit,BOMId,Quantity,FryingItem";

                SqlCommand cmd3 = new SqlCommand(Query2, conn2);
                SqlDataReader dr3 = cmd3.ExecuteReader();



                if (dr3.HasRows)
                {
                    while (dr3.Read())
                    {
                        decimal BQty = Convert.ToDecimal(dr3["Quantity"].ToString());
                        decimal OrderQty = Convert.ToDecimal(Quantity);
                        decimal result = BQty * OrderQty;

                        string FryingItem = dr3["FryingItem"].ToString();
                        decimal OHQ;

                        SqlConnection conn3 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                        conn3.Open();

                        string ItemQuery = @"Select * from Item where ItemId='" + FryingItem + @"'";

                        SqlCommand itemcmd = new SqlCommand(ItemQuery, conn3);
                        SqlDataReader itemdr = itemcmd.ExecuteReader();

                        if (itemdr.HasRows)
                        {
                            while (itemdr.Read())
                            {
                                decimal OHQR;
                                if (ReturnTransactionID == "")
                                {
                                    OHQ = Convert.ToDecimal(itemdr["OnHandQuantity"] == DBNull.Value ? "0" : itemdr["OnHandQuantity"].ToString());
                                    OHQR = OHQ - result;

                                    SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                    conn4.Open();

                                    string ItemUpdateQuery = @"Update Item Set OnHandQuantity='" + OHQR + @"' Where ItemId='" + FryingItem + @"'";

                                    SqlCommand itemupdatecmd = new SqlCommand(ItemUpdateQuery, conn4);
                                    SqlDataReader itemupdatedr = itemupdatecmd.ExecuteReader();
                                    itemupdatedr.Close();
                                    conn4.Close();
                                }
                                else
                                {
                                    // Return order
                                    OHQ = Convert.ToDecimal(itemdr["OnHandQuantity"] == DBNull.Value ? "0" : itemdr["OnHandQuantity"].ToString());
                                    OHQR = OHQ - result;

                                    SqlConnection connOr = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                    connOr.Open();

                                    string OrQuery = @"Update Orders Set OrderStatus='Cancelled', OrderState='Cancelled' where TransactionID='" + ReturnTransactionID + @"'";

                                    SqlCommand Orcmd = new SqlCommand(OrQuery, connOr);
                                    SqlDataReader Ordr = Orcmd.ExecuteReader();
                                    Ordr.Close();
                                    connOr.Close();

                                    if (OrderStatusID != "2")
                                    {
                                        SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                        conn4.Open();

                                        string ItemUpdateQuery = @"Update Item Set OnHandQuantity='" + OHQR + @"' Where ItemId='" + FryingItem + @"'";

                                        SqlCommand itemupdatecmd = new SqlCommand(ItemUpdateQuery, conn4);
                                        SqlDataReader itemupdatedr = itemupdatecmd.ExecuteReader();
                                        itemupdatedr.Close();
                                        conn4.Close();
                                    }
                                }
                            }
                        }
                        itemdr.Close();
                        conn3.Close();
                    }
                }
                else
                {
                    if (ReturnTransactionID != "")
                    {
                        SqlConnection connOr = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                        connOr.Open();

                        string OrQuery = @"Update Orders Set OrderStatus='Cancelled', OrderState='Cancelled' where TransactionID='" + ReturnTransactionID + @"'";

                        SqlCommand Orcmd = new SqlCommand(OrQuery, connOr);
                        SqlDataReader Ordr = Orcmd.ExecuteReader();
                        Ordr.Close();
                        connOr.Close();
                    }
                }

                dr3.Close();
                conn2.Close();

                WriteToFile("Order Successfully Created: OrderID = " + OrderID);
            }
            catch (Exception ex)
            {
                WriteToFile(ex.Message + " OrderId = " + OrderID);
            }
        }
        public void UpdateHDSOrder(string TransactionType, string HDSOrderID, string ItemID)
        {
            SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
            conn.Open();

            string Query = @"Update Orders Set OnHold='false',TransactionType='" + TransactionType + "' where HDSOrderId='" + HDSOrderID + "' and ItemID='" + ItemID + "'";

            SqlCommand cmd = new SqlCommand(Query, conn);
            SqlDataReader dr = cmd.ExecuteReader();

            dr.Close();
            conn.Close();

        }
        public void UpdateVoidSuspendedOrder(string HDSOrderID, string OrderItemID, string OrderTypeID, decimal Quantity)
        {
            SqlConnection conn2 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
            conn2.Open();
            string Query2 = @"Select ItemId,Unit,BOMId,SUM(Quantity) Quantity,FryingItem from BOM where ItemId='" + OrderItemID + @"'
                              group by ItemId,Unit,BOMId,Quantity,FryingItem";
            SqlCommand cmd3 = new SqlCommand(Query2, conn2);
            SqlDataReader dr3 = cmd3.ExecuteReader();

            if (dr3.HasRows)
            {
                while (dr3.Read())
                {
                    decimal BQty = Convert.ToDecimal(dr3["Quantity"].ToString());
                    decimal OrderQty = Convert.ToDecimal(Quantity);
                    decimal result = BQty * OrderQty;

                    string FryingItem = dr3["FryingItem"].ToString();
                    decimal OHQ;
                    decimal OHQR = 0;
                    SqlConnection conn3 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                    conn3.Open();

                    string ItemQuery = @"Select * from Item where ItemId='" + FryingItem + @"'";

                    SqlCommand itemcmd = new SqlCommand(ItemQuery, conn3);
                    SqlDataReader itemdr = itemcmd.ExecuteReader();

                    if (itemdr.HasRows)
                    {
                        while (itemdr.Read())
                        {
                            OHQ = Convert.ToDecimal(itemdr["OnHandQuantity"] == DBNull.Value ? "0" : itemdr["OnHandQuantity"].ToString());
                            OHQR = OHQ + result;
                        }
                        if (OrderStatusID != "2")
                        {
                            SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                            conn4.Open();

                            string ItemUpdateQuery = @"Update Item Set OnHandQuantity='" + OHQR + @"' Where ItemId='" + FryingItem + @"'";

                            SqlCommand itemupdatecmd = new SqlCommand(ItemUpdateQuery, conn4);
                            SqlDataReader itemupdatedr = itemupdatecmd.ExecuteReader();
                            itemupdatedr.Close();
                            conn4.Close();
                        }
                    }
                    itemdr.Close();
                    conn3.Close();
                }
            }
            dr3.Close();
            conn2.Close();

            SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
            conn.Open();

            string Query = @"Update Orders Set  OrderStatus='Cancelled' ,OrderState='Cancelled' where HDSOrderId='" + HDSOrderID + "'";

            SqlCommand cmd = new SqlCommand(Query, conn);
            SqlDataReader dr = cmd.ExecuteReader();

            dr.Close();
            conn.Close();

        }
        public void GetItemForecastData()
        {
            try
            {
                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSRetail"].ConnectionString);
                conn.Open();
                string query = @"Select FRYINGITEM ,ifor.FROMDATETIME_FORECASTING, ifor.TODATETIME_FORECASTING,sum(ifor.FRYINGHISTORICALQTY) ForecastQty, sum(ifor.PROJECTIONQTY) ProjectedFryQty
                                 from ext.MZNKDITEMFORECAST ifor
                                 Group by FRYINGITEM, ifor.FROMDATETIME_FORECASTING, ifor.TODATETIME_FORECASTING";
                SqlCommand cmd = new SqlCommand(query, conn);
                SqlDataReader dr = cmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        try
                        {
                            string FromDate = dr["FROMDATETIME_FORECASTING"].ToString();
                            DateTime datefrom = Convert.ToDateTime(FromDate).AddHours(5);
                            string From = datefrom.ToString("MM-dd-yyyy HH:MM:ss", CultureInfo.InvariantCulture);

                            string TODate = dr["TODATETIME_FORECASTING"].ToString();
                            DateTime dateto = Convert.ToDateTime(TODate).AddHours(5);
                            string To = dateto.ToString("MM-dd-yyyy HH:MM:ss", CultureInfo.InvariantCulture);

                            string Item = dr["FRYINGITEM"].ToString();
                            string ForecastQuantity = dr["ForecastQty"].ToString();
                            string ProjectedQuantity = dr["ProjectedFryQty"].ToString();

                            SqlConnection conn2 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                            conn2.Open();

                            string Query = @"insert into dbo.ItemForecast(FromDate,ToDate,ItemID,ForecastedQuantity,ProjectedQuantity) 
                                        values('" + From + "','" + To + "','" + Item + "','" + ForecastQuantity + "','" + ProjectedQuantity + "');";

                            SqlCommand cmd2 = new SqlCommand(Query, conn2);
                            SqlDataReader dr2 = cmd2.ExecuteReader();

                            dr2.Close();
                            conn2.Close();

                            WriteToFile("Item Forecast Successfully Created");
                        }
                        catch (Exception ex)
                        {
                            WriteToFile(ex.Message);
                        }
                    }
                }
                else
                {
                    this.WriteToFile("No data found in Retail DB");
                }

                dr.Close();
                conn.Close();
            }
            catch (Exception ex)
            {
                WriteToFile(ex.Message);
            }
        }
        public string CheckSuspendedOrderPOS(string Transactionid)
        {
            string Suspended = "";
            SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
            conn.Open();
            string query = @"Select HDSOrderId from Orders Where HDSOrderId='" + Transactionid + "'";

            SqlCommand cmd = new SqlCommand(query, conn);
            SqlDataReader dr = cmd.ExecuteReader();

            if (dr.HasRows)
            {
                while (dr.Read())
                {
                    Suspended = dr["HDSOrderId"].ToString();
                }
            }

            dr.Close();
            conn.Close();

            return Suspended;
        }
        public string GetOrderID(string hdsorderid)
        {
            string Orderids = "";
            SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
            conn.Open();
            string query = @"Select OrderId from Orders Where HDSOrderId='" + hdsorderid + "'";

            SqlCommand cmd = new SqlCommand(query, conn);
            SqlDataReader dr = cmd.ExecuteReader();

            if (dr.HasRows)
            {
                while (dr.Read())
                {
                    Orderids = dr["OrderId"].ToString();
                }
            }

            dr.Close();
            conn.Close();

            return Orderids;
        }
        public void UpdateOrderStatus(string Status, string ID)
        {
            SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
            conn.Open();
            string Query = @"Update Orders Set NextOrderStatus='" + Status + "' where OrderId='" + ID + @"'
                             Update Orders SET OrderStatus = CASE
                             WHEN NextOrderStatus = 0 THEN 'Frying'
                             WHEN NextOrderStatus = 1 THEN 'Preparation'
							 WHEN NextOrderStatus = 2 THEN 'Expeditor'
                             WHEN NextOrderStatus = 3 THEN 'FOH2'
                             END, 
                             OrderState = CASE
                             WHEN OrderStatusID in (0,1,2,3) THEN 'Preparing'
                             END
                             where NextOrderStatus >= 0 and NextOrderStatus <= 5 and(orderID = '" + ID + "')";

            SqlCommand cmd = new SqlCommand(Query, conn);
            SqlDataReader dr = cmd.ExecuteReader();
            dr.Close();
            conn.Close();
        }
        public void ResetBlinkingOrders()
        {
            try
            {
                SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                conn.Open();

                String query = @"Update Orders set OrderStatusID=NextOrderStatus where NextOrderStatus !=''
                                     Update Orders SET OrderStatus = CASE 
                                     WHEN OrderStatusID = 0 THEN 'Frying'
                                     WHEN OrderStatusID = 1 THEN 'Preparation'
                                     WHEN OrderStatusID = 2 THEN 'Expeditor'
                                     WHEN OrderStatusID = 3 THEN 'FOH2'
                                     WHEN OrderStatusID = 4 THEN 'Customer'
                                     WHEN OrderStatusID = 5 THEN 'Delivered'
                                     END, 
                                     OrderState= CASE
                                     WHEN OrderStatusID in (0,1,2,3) THEN 'Preparing'
                                     WHEN OrderStatusID = 4 THEN 'Ready'
                                     WHEN OrderStatusID = 5 THEN 'Fulfilled'
                                     WHEN OrderStatusID = 6 THEN 'Cancelled'
                                     END
                                     where Orderstatusid >=  0 and Orderstatusid <= 5 and (orderID in (Select OrderID from Orders where NextOrderStatus !=''))
                                     Update Orders set NextOrderStatus = NULL where NextOrderStatus !=''";

                SqlCommand cmd = new SqlCommand(query, conn);
                SqlDataReader dr = cmd.ExecuteReader();
                dr.Close();
                conn.Close();


            }
            catch (Exception ex)
            {
                WriteToFile(ex.ToString());
            }
        }
        public void DeleteSuspendedOrder(string HDSOID, string Oid)
        {
            SqlConnection con3 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
            con3.Open();

            string Query3 = @"Select * from Orders where HDSOrderId = '" + HDSOID + "'";

            SqlCommand cmd3 = new SqlCommand(Query3, con3);
            SqlDataReader dr3 = cmd3.ExecuteReader();

            if (dr3.HasRows && HDSOID != "")
            {
                while (dr3.Read())
                {
                    string OrderItemID = dr3["ItemID"].ToString();
                    string OrderTypeID = dr3["OrderTypeID"].ToString();
                    string Quantity = dr3["Quantity"].ToString();
                    string OSID = dr3["OrderStatusID"].ToString();

                    SqlConnection conni = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                    conni.Open();
                    string Queryi = @"Select ItemId,Unit,BOMId,SUM(Quantity) Quantity,FryingItem from BOM where ItemId='" + OrderItemID + @"'
                              group by ItemId,Unit,BOMId,Quantity,FryingItem";
                    SqlCommand cmdi = new SqlCommand(Queryi, conni);
                    SqlDataReader dri = cmdi.ExecuteReader();

                    if (dri.HasRows)
                    {
                        while (dri.Read())
                        {
                            decimal BQty = Convert.ToDecimal(dri["Quantity"].ToString());
                            decimal OrderQty = Convert.ToDecimal(Quantity);
                            decimal result = BQty * OrderQty;

                            string FryingItem = dri["FryingItem"].ToString();
                            decimal OHQ;
                            decimal OHQR = 0;
                            SqlConnection conn3 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                            conn3.Open();

                            string ItemQuery = @"Select * from Item where ItemId='" + FryingItem + @"'";

                            SqlCommand itemcmd = new SqlCommand(ItemQuery, conn3);
                            SqlDataReader itemdr = itemcmd.ExecuteReader();

                            if (itemdr.HasRows)
                            {
                                while (itemdr.Read())
                                {
                                    OHQ = Convert.ToDecimal(itemdr["OnHandQuantity"] == DBNull.Value ? "0" : itemdr["OnHandQuantity"].ToString());
                                    OHQR = OHQ + result;
                                }

                                if (OSID != "2")
                                {
                                    SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                                    conn4.Open();

                                    string ItemUpdateQuery = @"Update Item Set OnHandQuantity='" + OHQR + @"' Where ItemId='" + FryingItem + @"'";

                                    SqlCommand itemupdatecmd = new SqlCommand(ItemUpdateQuery, conn4);
                                    SqlDataReader itemupdatedr = itemupdatecmd.ExecuteReader();
                                    itemupdatedr.Close();
                                    conn4.Close();
                                }
                            }
                            itemdr.Close();
                            conn3.Close();
                        }
                    }
                    dri.Close();
                    conni.Close();
                }
            }

            SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
            conn.Open();

            String query = @"Delete from Orders where HDSOrderId='" + HDSOID + @"'
                             Delete from ItemTransLog where OrderId='" + Oid + "'";

            SqlCommand cmd = new SqlCommand(query, conn);
            SqlDataReader dr = cmd.ExecuteReader();
            dr.Close();
            conn.Close();
        }
        //dk add func
        public void GetduplicateOrderLine(string OrderId, string ItemId, string lINENUM)
        {

            SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
            conn.Open();
            string query = @"Select COUNT(*) from Orders  Where OrderID=' " + OrderId + "' and ItemID='" + ItemId + "' and LINENUM='" + lINENUM + "'";

            SqlCommand cmd = new SqlCommand(query, conn);
            SqlDataReader dr = cmd.ExecuteReader();

            if (dr.HasRows)
            {
                while (dr.Read())
                {
                    dr.Close();
                    conn.Close();
                    break;
                }
            }

            dr.Close();
            conn.Close();
        }

        public void UpdateOrderTime(string HDSOID, string CreatedOn)
        {
            if (HDSOID != "")
            {
                SqlConnection conn4 = new SqlConnection(ConfigurationManager.ConnectionStrings["CSKDS"].ConnectionString);
                conn4.Open();

                string ItemUpdateQuery = @"Update Orders Set CreatedOn='" + CreatedOn + @"' Where HDSOrderId='" + HDSOID + @"'";

                SqlCommand itemupdatecmd = new SqlCommand(ItemUpdateQuery, conn4);
                SqlDataReader itemupdatedr = itemupdatecmd.ExecuteReader();
                itemupdatedr.Close();
                conn4.Close();
            }

        }

    }
}
