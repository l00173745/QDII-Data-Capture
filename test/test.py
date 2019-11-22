import unittest
import pandas as pd
import numpy as np
import pymysql
from common.DB_mysql import DB_mysql

from datetime import datetime


class QDIITest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mysql = DB_mysql()
        # mysql.conn = pymysql.connect(host="LITO9-W10", user="MYSQLSupp",password="Duanka_0904",database="world",charset="utf8")
        print("-------------------------------------------------------------------------------------")
        print("Start test")

    @classmethod
    def tearDownClass(cls):
        cls.test_module = None

    def test_DB_connection(self):

        sql_str = 'select * from city'
        df = self.mysql.query(sql_str)
        tbl_nme = 'world.city'
        data = {
            'ID': 5001,
            'NAME': 'TonyCity',
            'COUNTRYCODE': 'AFG',
            'DISTRICT': 'Herat',
            'POPULATION': 190000
        }
        mysql.insert(tbl_nme, data)
        pass



    # def test_negative_onboard(self):
    #     """
    #     Case 1 : negative onboard checking : when vsl arrival and dsch , the onboard teu should not negative
    #     case 2 : all voy_stop_cde in vessel_network
    #     """
    #     # ld_snapshot_all_rem = pd.read_csv(os.path.join(self.appFolderPath.get_msra_testdata_path(), "ld_snapshot_all_rem.csv"))
    #     self.assertTrue(self.ld_snapshot_all_rem[self.ld_snapshot_all_rem['EST_LD_OB_TEU']<0].empty)
    #     self.ld_snapshot_all_rem.sort_values(by=['VSL_CDE','SIZE_TYPE','SNAP_TICK','ARR_TICK'],inplace=True)
    #     self.ld_snapshot_all_rem['P_EST_LD_OB_TEU'] = self.ld_snapshot_all_rem.groupby(['VSL_CDE', 'SIZE_TYPE','SNAP_TICK'])['EST_LD_OB_TEU'].shift(1)
    #     self.ld_snapshot_all_rem['P_EST_LD_OB_WT'] = self.ld_snapshot_all_rem.groupby(['VSL_CDE', 'SIZE_TYPE','SNAP_TICK'])['EST_LD_OB_WT'].shift(1)
    #     self.ld_snapshot_all_rem['P_EST_LD_OB_TEU'] = np.where(pd.isnull(self.ld_snapshot_all_rem.P_EST_LD_OB_TEU), self.ld_snapshot_all_rem.INIT_LD_OB_TEU,
    #                                               self.ld_snapshot_all_rem.P_EST_LD_OB_TEU)
    #     self.ld_snapshot_all_rem['P_EST_LD_OB_WT'] = np.where(pd.isnull(self.ld_snapshot_all_rem.P_EST_LD_OB_WT), self.ld_snapshot_all_rem.INIT_LD_OB_WT,
    #                                              self.ld_snapshot_all_rem.P_EST_LD_OB_WT)
    #     self.ld_snapshot_all_rem['ARR_OB_TEU'] = self.ld_snapshot_all_rem['P_EST_LD_OB_TEU'] - self.ld_snapshot_all_rem['EST_DSCH_TEU']
    #     # self.ld_snapshot_all_rem['ARR_OB_TEU'] = self.ld_snapshot_all_rem['P_EST_LD_OB_TEU'] - self.ld_snapshot_all_rem['EST_DSCH_TEU']
    #     self.ld_snapshot_all_rem['ARR_OB_TEU'] = self.ld_snapshot_all_rem['PRE_SUM_DELTA_TEU'] - \
    #                                              self.ld_snapshot_all_rem[
    #                                                  'DELTA_TEU'] + self.ld_snapshot_all_rem['INIT_LD_OB_TEU']
    #     self.assertTrue(self.ld_snapshot_all_rem[self.ld_snapshot_all_rem['ARR_OB_TEU'] < 0].empty)
    #     voy_stop_lst = list(set(self.proforma_vessel_network['VOY_STOP_CDE'].tolist()))
    #     self.assertTrue(all(self.ld_snapshot_all_rem['VOY_STOP_CDE'].isin(voy_stop_lst)))
    #
    # def test_negative_fcil_mta(self):
    #     """
    #     Case 1 : negative mta checking : if there is any mat<-10
    #     Case 2 : sum(delta) + init_mta = last tick mta
    #     """
    #     self.assertTrue(self.fcil_mta[self.fcil_mta['MTA'] < 0].empty)
    #     # df_sum_delta=self.fcil_mta.groupby(by=['SIZE_TYPE', 'FCIL']).agg({'DELTA': sum}).reset_index()
    #     # df_sum_delta['FINAL']=df_sum_delta[]
    #
    # def test_negative_opz_mta(self):
    #     """
    #     Case 1 : negative mta checking : if there is any mat<-100
    #     """
    #     df_opz = self.fcil_mta.groupby(by=['OPZ', 'SIZE_TYPE', 'TICK']).agg({
    #         'MT_OTHER_IN': sum,
    #         'MT_OTHER_OUT': sum,
    #         'MT_PLAN_IN': sum,
    #         'MT_PLAN_OUT': sum,
    #         'MT_RLS': sum,
    #         'MT_RTN': sum,
    #         'DELTA': sum,
    #         'INIT_OB': max
    #     }).reset_index()
    #     df_init = self.fcil_mta.groupby(by=['OPZ', 'SIZE_TYPE', 'FCIL']).agg({
    #         'INIT_MTA': max
    #     }).reset_index()
    #     df_init = df_init.groupby(by=['OPZ', 'SIZE_TYPE']).agg({
    #         'INIT_MTA': sum
    #     }).reset_index()
    #     df_opz = df_opz.merge(df_init, on=['OPZ', 'SIZE_TYPE'])
    #     df_opz['MTA'] = df_opz.groupby(['OPZ', 'SIZE_TYPE'])['DELTA'].cumsum() + df_opz['INIT_MTA']
    #     print(df_opz[(df_opz['MTA'] < -30) & (df_opz['TICK']<43000)] )
    #     self.assertTrue(df_opz[df_opz['MTA'] < 0].empty)
    #
    #
    #
    # def test_vsl_nw(self):
    #     # proforma_vessel_network = pd.read_csv(os.path.join(self.appFolderPath.get_msra_result_path(), "proforma_vessel_network.csv"))
    #     proforma_vessel_network = self.proforma_vessel_network[~self.proforma_vessel_network['VOY_STOP_CDE'].isnull()]
    #     voy_stop_cost_lst = list(set(self.vs_terminal_cost['VOY_STOP_CDE'].tolist()))
    #     self.assertTrue(all(proforma_vessel_network['VOY_STOP_CDE'].isin(voy_stop_cost_lst)))
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['SEA_HR']<0].empty)
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['PORT_HR'] < 0].empty)
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['CAP_TEU'] < 0].empty)
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['CAP_WT'] < 0].empty)
    #     result_teu_max = proforma_vessel_network['CAP_TEU'].groupby(proforma_vessel_network['VSL_CDE']).max()
    #     result_teu_min = proforma_vessel_network['CAP_TEU'].groupby(proforma_vessel_network['VSL_CDE']).min()
    #     self.assertTrue(any(result_teu_max / result_teu_min) < 10)
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['FCIL_MTA_20GP'] < 0].empty)
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['FCIL_MTA_40GP'] < 0].empty)
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['FCIL_MTA_40HQ'] < 0].empty)
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['OPZ_MTA_20GP'] < 0].empty)
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['OPZ_MTA_40GP'] < 0].empty)
    #     self.assertTrue(proforma_vessel_network[proforma_vessel_network['OPZ_MTA_40HQ'] < 0].empty)
    #
    #
    # def test_init_mt_ob(self):
    #     # initial_mt_ob = pd.read_csv(os.path.join(self.appFolderPath.get_msra_result_path(), "initial_mt_ob.csv"))
    #     initial_mt_ob = self.initial_mt_ob
    #     self.assertTrue(all(initial_mt_ob['QTY']) > 0)
    #     self.assertTrue(all(initial_mt_ob['CNTR_TEU']) > 0)
    #     self.assertTrue(all(initial_mt_ob['CNTR_TEU']) < 3000)
    #     self.assertTrue(all(initial_mt_ob['CNTR_WT_TON']) > 0)
    #     self.assertTrue(all(initial_mt_ob['CNTR_WT_TON']) <= 3700)
    #     self.assertFalse(any(initial_mt_ob[['DSCH_VOY_STOP_CDE', 'SIZE_TYPE']].duplicated()))
    #
    #
    # def test_init_ld_ob(self):
    #     # initial_ld_ob = pd.read_csv(os.path.join(self.appFolderPath.get_msra_result_path(), "initial_ld_ob.csv"))
    #     initial_ld_ob = self.initial_ld_ob
    #     self.assertFalse(initial_ld_ob.VSL_ID.isna().any())
    #     self.assertFalse(any(initial_ld_ob['VSL_ID'].duplicated()))
    #     self.assertTrue(all(initial_ld_ob['INIT_LD_OB_QTY']) > 0)
    #     self.assertTrue(all(initial_ld_ob['INIT_LD_OB_TEU']) > 0)
    #     self.assertTrue(all(initial_ld_ob['INIT_LD_OB_WT']) > 0)
    #
    #
    # def test_loc(self):
    #     # loc = pd.read_csv(os.path.join(self.appFolderPath.get_msra_result_path(), "loc.csv"))
    #     loc = self.loc
    #     self.assertIsNotNone(loc['OPZ_CDE'])
    #     self.assertIsNotNone(loc['COUNTRY_CDE'])
    #     self.assertIsNotNone(loc['CONTINENT_CDE'])
    #     self.assertIsNotNone(loc['IS_FEEDBACK_AGENT'])
    #     self.assertFalse(any(loc['OPZ_ID'].duplicated()))
    #
    #
    # def test_user_mt_plan(self):
    #     # mt_user_plan = pd.read_csv(os.path.join(self.appFolderPath.get_msra_result_path(), "mt_user_plan.csv"))
    #     mt_user_plan = self.mt_user_plan
    #     voy_stop_lst = list(set(self.proforma_vessel_network['VOY_STOP_CDE'].tolist()))
    #     self.assertFalse(mt_user_plan.SIZE_TYPE_ID.isna().any())
    #     self.assertTrue(all(mt_user_plan['LOAD_VOY_STOP_CDE'].isin(voy_stop_lst)))
    #     self.assertTrue(all(mt_user_plan['DSCH_VOY_STOP_CDE'].isin(voy_stop_lst)))
    #
    #
    #
    # def test_port_initial_mta(self):
    #     # port_initial_mta = pd.read_csv(os.path.join(self.appFolderPath.get_msra_result_path(), "port_initial_mta.csv"))
    #     port_initial_mta = self.port_initial_mta
    #     self.assertFalse(port_initial_mta['PORT_CDE'].isna().any())
    #     self.assertFalse(port_initial_mta['SIZE_TYPE'].isna().any())
    #     self.assertFalse(port_initial_mta['QTY'].isna().any())
    #     self.assertFalse(any(port_initial_mta[['PORT_CDE', 'SIZE_TYPE']].duplicated()))
    #     self.assertTrue(all(port_initial_mta['QTY'] >= 0))
    #
    # def test_snd(self):
    #     # snd = pd.read_csv(os.path.join(self.appFolderPath.get_msra_result_path(), "snd.csv"))
    #     snd = self.snd
    #     # opz_lst =  list(set(self.proforma_vessel_network[~self.proforma_vessel_network['VOY_STOP_CDE'].isna()]['OPZ_CDE'].tolist()))
    #     # opz_lst = list(set(self.proforma_vessel_network['OPZ_CDE'].tolist()))
    #     fcil_lst=list(set(self.port_initial_mta['PORT_CDE'].tolist()))
    #     self.assertFalse(snd['SND_TYPE_CDE'].isna().any())
    #     self.assertFalse(snd['SIZE_TYPE'].isna().any())
    #     self.assertFalse(snd['SND_TYPE_SEQ'].isna().any())
    #     self.assertFalse(snd['QTY'].isna().any())
    #     self.assertEquals(snd[(~snd['FROM_PORT'].isna()) & (snd['FROM_TICK'].isna())]['FROM_TICK'].count(), 0)
    #     self.assertEquals(snd[(~snd['TO_PORT'].isna()) & (snd['TO_TICK'].isna())]['TO_TICK'].count(), 0)
    #     self.assertFalse(snd[['SND_TYPE_CDE', 'SIZE_TYPE', 'FROM_PORT', 'TO_PORT', 'SND_TYPE_SEQ', 'FROM_TICK',
    #                              'TO_TICK']].duplicated().any())
    #     self.assertTrue(snd['QTY'].all() >= 0)
    #     # self.assertTrue(all(snd['FROM_PORT'].isin(opz_lst) | snd['TO_PORT'].isin(opz_lst)))
    #     self.assertTrue(all(snd['FROM_PORT'].isin(fcil_lst) | snd['TO_PORT'].isin(fcil_lst)))
    #
    #
    #
    # # def test_overload(self):
    # #     vsl_lst = list(set(self.proforma_vessel_network['VSL_CDE'].tolist()))
    # #     # for vsl in vsl_lst:
    # #     for vsl in ['XYZ']:
    # #         nw_vsl = self.proforma_vessel_network[self.proforma_vessel_network['VSL_CDE']==vsl]
    # #         nw_vsl = nw_vsl[['VSL_CDE','VOY_STOP_CDE','CAP_TEU','CAP_WT']]
    # #         rem_vsl = self.ld_snapshot_all_rem[self.ld_snapshot_all_rem['VSL_CDE']==vsl]
    # #         rem_vsl = rem_vsl[['VOY_STOP_CDE','SNAP_TICK','EST_LD_OB_TEU','EST_LD_OB_WT']]
    # #         rem_vsl_tick_lst = list(set(rem_vsl['SNAP_TICK'].tolist()))
    # #         for tick in rem_vsl_tick_lst:
    # #             # if tick==6144:
    # #             #     print('test')
    # #             rem_vsl_tick = rem_vsl[rem_vsl['SNAP_TICK']==tick]
    # #             nw_tick = nw_vsl.merge(rem_vsl_tick,on='VOY_STOP_CDE',how='left')
    # #             nw_tick = nw_tick.ffill()
    # #             nw_tick.fillna(0,inplace=True)
    # #             if not all(nw_tick['CAP_TEU'] >= nw_tick['EST_LD_OB_TEU']):
    # #                 nw_tick['TRUE'] = nw_tick['CAP_TEU'] >= nw_tick['EST_LD_OB_TEU']
    # #                 print('test')
    # #             print(all(nw_tick['CAP_TEU'] >= nw_tick['EST_LD_OB_TEU']))
    # #             self.assertTrue(all(nw_tick['CAP_TEU'] >= nw_tick['EST_LD_OB_TEU']))
    #
    # def test_overload(self):
    #     ld_snapshot_all_rem_w_cap = self.ld_snapshot_all_rem.merge(self.proforma_vessel_network[['VOY_STOP_CDE','CAP_TEU','CAP_WT']],on=['VOY_STOP_CDE'])
    #     ld_snapshot_all_rem_w_cap['TRUE'] = ld_snapshot_all_rem_w_cap['CAP_TEU'] >= ld_snapshot_all_rem_w_cap['EST_LD_OB_TEU']
    #     self.assertTrue(ld_snapshot_all_rem_w_cap[ld_snapshot_all_rem_w_cap['EST_LD_OB_TEU'] > ld_snapshot_all_rem_w_cap['CAP_TEU']].empty)
    #
    # def test_duplicate(self):
    #     custom_DSN = 'mtrepo_impala'
    #     hadoop = IteractWithHadoop()
    #     sql_str_lst = 'select event_uuid,count(1) from mtrepo.MSRA_DP_S3V1_post_with_prof_v7_train_dynamic_ver3 group by event_uuid having count(1)>1'
    #     df = hadoop.query(sql_str_lst, custom_DSN)
    #     self.assertTrue(df.empty)








if __name__ == '__main__':
    unittest.main()
