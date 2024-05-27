import aiohttp
import requests
from datetime import timedelta
from datetime import datetime
from datetime import date
import hmac
import hashlib
import asyncio
import adal
import json
from async_lru import alru_cache
import urllib.parse
import socket
import traceback
import sys
from lxml import etree as xml
from src.AzureMonitoring.msg_handler import *
import psycopg2
import psycopg2.extras
import xml.etree.ElementTree as ET
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from src.Utilities import atlogger
from src.AzureMonitoring.utils import *
from DBClasses import *
import urllib.request as urllib2
import ssl
import base64
from product_skuid_map import product_skuid_map
import os
import random
import concurrent.futures
from AzurePowershellExecutor import AzurePowershellExecutor

class AzureAppCred:
    def __init__(self, tenantid, applicationid, appsecret, productType, customerId,citrixId,citrixSecret,hzurl, hzoauthid, hzoauthsecret):
        self.tenantid = tenantid
        self.applicationid = applicationid
        self.appsecret = appsecret
        self.productType = productType
        self.customerId = customerId
        self.citrixId=citrixId
        self.citrixSecret = citrixSecret
        self.hzurl = hzurl
        self.hzoauthid = hzoauthid
        self.hzoauthsecret = hzoauthsecret


class AzureInfra:
    tenantid = None
    tenantname = None
    creds = None
    citrixId = None
    citrixSecret = None
    customerid = None
    subscriptions = []
    # It has value only if subscription level monitoring enabled.
    configured_subscription = None

    def __init__(self, tenantname, tenantid, applicationid, appsecret,productType,customerId,citrixId,citrixSecret,hzurl, hzoauthid, hzoauthsecret,configured_subscription=None):
        self.tenantid = tenantid
        self.tenantname = tenantname
        self.creds = AzureAppCred(tenantid, applicationid, appsecret ,productType,customerId,citrixId,citrixSecret,hzurl, hzoauthid, hzoauthsecret)
        self.configured_subscription = configured_subscription
        #atlogger.debug("@PG self.tenantname in AzureInfra class %s", tenantname)
        # List of subscription onject wilth all info
        self.subscriptions = []
        # List of subscriptions under azure tenant
        self.subscript_list = []

    async def fetch(self, client, headers, azure_mgmt_url):
        self.azure_mgmt_url = azure_mgmt_url
        self.subscriptions = await self.fetch_subscriptions(client, headers)
        print(self.subscriptions)

    async def fetch_subscriptions(self, client, headers):
        self.subscript_list = await self.get_subscriptions_list(client, headers)
        atlogger.debug("@PG self.tenantname in fetch_subscriptions %s",self.tenantname)
        for subscrid in self.subscript_list:
            subscr = AzureSubscription(self.tenantname, subscrid, self.creds)
            await subscr.fetch(client, headers, self.azure_mgmt_url)
            # print(subscr)
            self.subscriptions.append(subscr)
            # print(self.subscriptions)
        return self.subscriptions

    """ Function to get list of subscriptions """
    async def get_subscriptions_list(self, client, headers):
        atlogger.debug("Going to fetch subscriptions %s %s",self.azure_mgmt_url,client)
        if self.configured_subscription == None  or self.configured_subscription=="":
            sub_url = "{0}{1}".format(self.azure_mgmt_url, 'subscriptions')
            print(self.azure_mgmt_url)
            sub_params = {'api-version': '2019-06-01'}
            sub_details = await get_details_async(client, headers, sub_url, sub_params)
            atlogger.debug("sub_details is %s",sub_details)
            sub_list = []
            for sub in sub_details['value']:
                specific_sub = {}
                specific_sub['id'] = sub['subscriptionId']
                specific_sub['displayName'] = sub['displayName']
                specific_sub['state'] = sub['state']
                sub_list.append(specific_sub)
            # return list of sub dict
            return sub_list
        else:
            sub_url = "{0}{1}{2}".format(self.azure_mgmt_url, 'subscriptions/',
                                         self.configured_subscription)
            sub_params = {'api-version': '2019-06-01'}
            sub_details = await get_details_async(client, headers, sub_url,
                                                  sub_params)
            sub_list = []
            for sub in sub_details['value']:
                specific_sub = {}
                specific_sub['id'] = sub['subscriptionId']
                specific_sub['displayName'] = sub['displayName']
                specific_sub['state'] = sub['state']
                sub_list.append(specific_sub)
            '''specific_sub = {}
            specific_sub['id'] = sub_details['subscriptionId']
            specific_sub['displayName'] = sub_details['displayName']
            specific_sub['state'] = sub_details['state']
            sub_list.append(specific_sub)'''
            return sub_list

    def to_xml():
        pass

    def get_subscrid_for_vm(self, vmname):
        for subscr in self.subscriptions:
            if subscr.does_vm_exists(vmname):
                return subscr.subscription_id
        #TODO:Log a warning here.
        if len(self.subscriptions) > 0:
            return self.subscriptions[0].subscription_id

    def get_ipaddr_for_vm(self, vmname):
        for subscr in self.subscriptions:
            if subscr.does_vm_exists(vmname):
                return subscr.get_ipaddr_for_vm(vmname)
        #TODO:Log a warning here.
        return None

class AzureSubscription:
    def __init__(self, tenantname, subInfo, creds):
        self.subInfo = subInfo
        self.tenantname = tenantname
        self.subscription_id = self.subInfo.get('id') + '/resourceGroups/anunta-tst-rg01'
        self.sub_id = self.subInfo.get('id')
        self.display_name = self.subInfo.get('displayName')
        self.state = self.subInfo.get('state')
        self.creds = creds
        self.resourceGroups = []
        # Dict of resource group to List of resource Objects in that grp.
        self.resources = {}
        self.resources_list = {}
        self.nwk_inf = {}
        self.vcpu_usage = {}
        self.ad_sync = []
        self.citrix = CitrixDetails(self.subscription_id, self.display_name, self.creds)
        self.wvd = WVDDetails(self.tenantname, self.subscription_id, self.creds)
        self.reslist_dict = {}
        # Added for VMware Horizon Cloud on Azure V2
        self.horizon = horizonDetails(self.subscription_id,self.creds)

    """ Function to retrive resources/wvd details under subscription """
    async def fetch(self, client, headers, azure_mgmt_url):
        try:
            self.azure_mgmt_url = azure_mgmt_url
            #self.subscription_id = self.subscrid.get('id')
            print(self.subscription_id)
            self.resourceGroups = await self.fetch_resourcegroup_details(
                client, headers)
            atlogger.debug("resourceGroup fetch completed : %s", self.resourceGroups)
            print(self.resourceGroups)
            self.resources_list = await self.fetch_resources_details(
                client, headers)
            atlogger.debug("resources fetch completed : %s", self.resources_list)
            # print(self.resources_list)
            await self.fetch_resource_properties(client, headers)
            self.vcpu_usage = await self.fetch_resource_usage(
                    client, headers)
            atlogger.debug("vcpu usgae fetch completed : %s", self.vcpu_usage)
            print(self.vcpu_usage)
            self.ad_sync = await self.fetch_adsync_details()
            # Discover wvd details
            if self.creds.productType == 'Azure On Citrix Cloud':
                atlogger.debug("PRODUCT TYPE :%s", self.creds.productType)
                await self.citrix.citrix_fetch()
                #Added for VMware Horizon Cloud on Azure V2
            elif self.creds.productType == 'Horizon Cloud on Azure Titan':
                atlogger.debug("Product Type: %s",self.creds.productType)
                #await self.horizon.horizon_fetch()
                self.horizon.horizon_fetch()
            else:
                await self.wvd.fetch()
        except Exception as e:
            atlogger.error('Exception thrown while fetching azure infra'
                           'and wvd details')
            traceback.print_exception(*sys.exc_info())
            # return

    async def fetch_resourcegroup_details(self, client, headers):
        atlogger.debug("Going to fetch resource group details")
        rg_url = "https://management.azure.com/subscriptions/{}/resourcegroups"
        #rg_url = rg_url.format(self.subscription_id)
        rg_url = rg_url.format(self.sub_id)
        rg_params = {'api-version': '2020-06-01'}

        rg_details = await get_details_async(client, headers, rg_url, rg_params)

        rg_list = []
        for rg in rg_details['value']:
            res_grp = {}
            res_grp['name'] = rg.get('name')
            res_grp['location'] = rg.get('location')
            #res_grp['porvisioningState'] = rg.get(
            res_grp['provisioningState'] = rg.get(
                'properties').get('provisioningState')
            rg_list.append(res_grp)
        return rg_list

    async def fetch_resources_details(self, client, headers):
        # Fetch Resources
        try:
            print("Going to fetch resources details")
            res_url = self.azure_mgmt_url + 'subscriptions/' +\
                self.subscription_id + '/resources'
            res_params = {'api-version': '2020-06-01'}
            res_list = await get_details_async(client, headers,
                                               res_url, res_params)
            # print(res_list)
            var = 0
            params = ''
            if 'nextLink' not in res_list:
                print(len(res_list['value']))
                self.resources = await self.map_resources_with_resgroups(res_list)
                return res_list

            while 'nextLink' in res_list and\
                    res_list['nextLink'] != None:
                nxtres = res_list['nextLink']
                nxtres_list = await get_details_async(client, headers,
                                                      nxtres, params)
                res_list['value'].extend(nxtres_list['value'])
                res_list['nextLink'] = nxtres_list.get('nextLink')
                
                continue
            else:
                print("No remaining resource")
                print(len(res_list['value']))
                self.resources = await self.map_resources_with_resgroups(res_list)
                return res_list

        except Exception as e:
            # FIXME: At normal case it returns list resources, so then at failure it should return []
            # Otherwise should raise an error.
            print(str(e))
            traceback.print_exception(*sys.exc_info())

    #SD: Modified to eliminate unwanted resource details
    async def map_resources_with_resgroups(self, res_list):
        restype_to_class_mapping = {
            'Microsoft.Compute/disks': DiskInfo,
            'Microsoft.Compute/images': ImageInfo,
            'Microsoft.Network/networkInterfaces': NetworkInterfaceInfo,
            'Microsoft.Network/virtualNetworks': VirtualNetworkInfo,
            'Microsoft.Compute/virtualMachines': VirtualMachineInfo,
            'Microsoft.Network/publicIPAddresses': PublicIPAddressInfo,
            'Microsoft.Network/connections': VPNInfo,
            'Microsoft.Compute/availabilitySets': AvailabilitySetInfo,
            'Microsoft.Storage/storageAccounts': StorageInfo,
            'Microsoft.RecoveryServices/vaults': VaultInfo,
            'Microsoft.Network/networkSecurityGroups': NSGInfo,
        }
        resources = {}
        isVault = False
        for rg in self.resourceGroups:
            res_info = []  # Holds resources for this resourcegroup rg.
            for res in res_list['value']:
                res_prop = {}
                resname_list = []
                rid = res['id']
                rr = rid.split('/')
                rgName = rr[4]
                if rgName.lower() == rg['name'].lower():
                    res_name = res.get('name', '')
                    res_type = res.get('type', '')
                    res_loc = res.get('location', '')
                    atlogger.debug("res_name: %s, rgname: %s, res_type: %s", str(res_name), str(rgName.lower()), str(res_type))
                    if res_type in restype_to_class_mapping:
                        if res_type == 'Microsoft.RecoveryServices/vaults':
                            res_type = 'Microsoft/RecoveryServices'
                            isVault = True
                        if isVault:
                            Cls = restype_to_class_mapping['Microsoft.RecoveryServices/vaults']
                            atlogger.debug('Cls, %s', Cls)
                            isVault = False
                        else:    
                            Cls = restype_to_class_mapping[res_type]
                        re = Cls(rid, self.azure_mgmt_url,
                                 res_name, res_type, res_loc, rgName, self.tenantname)
                        atlogger.debug("resource mapped from dict")
                        """rgname = rgName.lower()
                        if self.reslist_dict.get(rgname) != None:
                            if str(res_name) in self.reslist_dict.get(rgname):
                                continue
                            resname_list = self.reslist_dict.get(rgname)
                        res_info.append(re)
                        resname_list.append(str(res_name))
                        self.reslist_dict[rgname] = resname_list
                        resources[rg['name']] = res_info"""
                    else:
                        exclude_list = [
                                #'Microsoft.Storage/storageAccounts',
                                ]
                        if res_type not in exclude_list:
                            continue
                        atlogger.debug("resource mapped from list")
                        re = DefaultTypeInfo(rid, self.azure_mgmt_url,
                                             res_name, res_type, res_loc, rgName, self.tenantname)
                    rgname = rgName.lower()
                    if self.reslist_dict.get(rgname) != None:
                        """if str(res_name) in self.reslist_dict.get(rgname):
                            continue"""
                        resname_list = self.reslist_dict.get(rgname)
                    res_info.append(re)
                    resname_list.append(str(res_name))
                    self.reslist_dict[rgname] = resname_list
                    resources[rg['name']] = res_info
                    # Testing purpose
                    #res_info.append(re)
                    # break
        #    resources[rg['name']] = res_info
        # print(resources)
        atlogger.debug("self.reslist_dict :%s", str(self.reslist_dict))
        return resources

    async def fetch_resource_properties(self, client, headers):
        try:
            atlogger.debug("fetch_resouce_properties...")
            tasks = []
            for resource in self.resources.values():
                for res in resource:
                    # print(res.name)
                    task = asyncio.ensure_future(res.fetch(client, headers))
                    tasks.append(task)
            await asyncio.gather(*tasks)

            for resource in self.resources.values():
                for res in resource:
                    # print(res.name)
                    await res.parse()

            await self.get_nwkinf_map()
            await self.assign_vmip_from_nwinf()
        except Exception as e:
            atlogger.error("Exception in fetch resource properties %s",str(e))
            traceback.print_exception(*sys.exc_info())

    async def fetch_resource_usage(self, client, headers):
        try:
            res_locations = [res.get('location') for res in
                             self.resources_list['value']]
            locations = list(set(res_locations))
            print(locations)
            loc_provider = '/providers/Microsoft.Compute/locations/'
            for location in locations:
                if location != "global":
                    """l_url = self.azure_mgmt_url + 'subscriptions/' + \
                        self.subscription_id + loc_provider + \
                        str(location) + '/usages'"""
                    l_url = self.azure_mgmt_url + 'subscriptions/' + \
                        self.sub_id + loc_provider + \
                        str(location) + '/usages'
                    l_params = {'api-version': '2019-07-01'}
                    us_list = await get_details_async(client, headers,
                                                      l_url, l_params)
                    self.vcpu_usage[location] = us_list
            return self.vcpu_usage
        except Exception as e:
            atlogger.error('Exception while fetching vcpu usage details')
            traceback.print_exception(*sys.exc_info())
            return {}

    async def fetch_adsync_details(self):
        try:
            graph_mgmt_url = 'https://graph.microsoft.com/'
            token = acquire_authtoken(self.creds.tenantid,
                                      self.creds.applicationid, self.creds.appsecret, graph_mgmt_url)
            headers = form_auth_headers(token)
            adsync = ADSyncData(self.creds)
            adsync_data = await adsync.fetch(headers)
            for data in adsync_data['value']:
                adsync = ADSyncData(self.creds)
                await adsync.parse(data)
                self.ad_sync.append(adsync)

            return self.ad_sync

        except Exception as e:
            atlogger.error('Exception in ADSync details')
            traceback.print_exception(*sys.exc_info())
            return []

    async def get_nwkinf_map(self):
        for resource in self.resources.values():
            for res in resource:
                if res.type == 'Microsoft.Network/networkInterfaces':
                    map_dict = {}
                    map_dict['ipAddress'] = res.privateIPAddress
                    map_dict['vnwkname'] = res.virtualNwName
                    map_dict['nsgName'] = res.nsgName
                    map_dict['publicIPName'] = res.publicIPName
                    self.nwk_inf[res.name] = map_dict

    async def assign_vmip_from_nwinf(self):
        print("Assigning vmip")
        for resource in self.resources.values():
            for res in resource:
                if res.type == 'Microsoft.Compute/virtualMachines':
                    val = self.nwk_inf[res.networkInterfaceName]
                    res.vmIP = val.get('ipAddress', "")
                    res.virtualNetworkName = val.get('vnwkname', "")
                    res.networkSecurityGroupName = val.get('nsgName', "")
                    res.publicIPAddressName = val.get('publicIPName', "")

    def to_xml(self,param):
        print("Going to generate response")
        subxml = xml.Element('subscription')

        # Adding basic subscription details.
        subxml.append(xml_tag('id', self.sub_id))
        subxml.append(xml_tag('displayName', self.display_name))
        subxml.append(xml_tag('state', self.state))
        
        if 'resourceGroupDetails' in param:
            rgrps = xml.Element('resourceGroupDetails')
            print("Adding resource group info")
            # Addinging resource groups info.
            for rg in self.resourceGroups:
                rgxml = xml.Element('resourceGroup')
                rgxml.append(xml_tag('name', rg.get('name')))
                rgxml.append(xml_tag('location', rg.get('location')))
                rgxml.append(xml_tag('provisioningState', rg.get('provisioningState')))
                # Adding resources under this resource group.
                rgxml.append(self.get_resources_xml_for_rg(rg.get('name')))
                # Adding this resource group to resourcegroups.
                rgrps.append(rgxml)
                # Adding resource group details under subscription.
                subxml.append(rgrps)
            return subxml, []
        if 'wvdDetails'in param:
            flag=0
            print("Adding wvd details")
            # Adding WVDDetails under subscription
            sub, lic = self.wvd.to_xml(flag)
            subxml.append(sub)
            return subxml,lic
        if 'usersProvisioned' in param:
            flag=1
            print("Adding usersProvisioned details")
            # Adding userprovisoned details
            print("Adding user data")
            sub, lic = self.wvd.to_xml(flag)
            subxml.append(sub)
            return subxml, []
        if 'usageInfo'in param:
            print("Adding vcpu usage")
            # Adding vcpu usage info under subscription
            usagexml = xml.Element('usageInfo')
            for region in self.vcpu_usage:
                usages = self.vcpu_usage[region]['value']
                usagexml.append(self.get_vcpu_usage_xml(region, usages))
            subxml.append(usagexml)
            print("Adding ADSync data")
            # Adding ADSync details under subscription
            adxml = xml.Element('ADSync')
            for ad in self.ad_sync:
                subxml.append(ad.to_xml(adxml))
            return subxml, []
        
        if 'citrixDetails' in param:
            flag = 2
            atlogger.debug("Adding citrix details")
            subxml.append(self.citrix.to_xml(flag))
            atlogger.debug("CITRIX XML RESPONSE:%s",subxml)
            return subxml, []

        if 'CitrixUsersProvisioned' in param:
            atlogger.debug("Adding Usersprovisioned citrix")
            flag=3
            subxml.append(self.citrix.to_xml(flag))
            return subxml,[]  

        if 'TitanWvdDetails' in param:
            flag = 4
            atlogger.debug("Adding Titan WvdDetails details")
            subxml.append(self.horizon.to_xml(flag))
            subxml.append(self.horizon.servers_xml(flag))
            subxml.append(self.horizon.images_xml(flag))
            atlogger.debug("HORIZON XML RESPONSE:%s",subxml)
            return subxml,[]
        
        if 'HorizonUsersProvisioned' in param:
            atlogger.debug("Adding Usersprovisioned Horizon")
            flag=5
            subxml.append(self.horizon.to_xml(flag))
            return subxml,[]

    def get_resources_xml_for_rg(self, rgname):
        atlogger.debug("Resource Details:%s",self.resources)
        rinfo = xml.Element('resourceInfo')
        if rgname not in self.resources:
            return rinfo
        for r in self.resources[rgname]:
            atlogger.debug("Resource loopdetails:%s",r)
            newxml=r.to_xml()
            if newxml is not None:
                rinfo.append(newxml)
        return rinfo

    def get_vcpu_usage_xml(self, region, usages):
        regxml = xml.Element('region')
        regxml.append(xml_tag('location', region))
        for usage in usages:
            usgxml = xml.Element('usage')
            name_val = usage.get('name', {})
            usgxml.append(xml_tag('name', name_val.get('value')))
            usgxml.append(xml_tag('localizedValue',
                                  name_val.get('localizedValue')))
            usgxml.append(xml_tag('limit', usage.get('limit')))
            usgxml.append(xml_tag('unit', usage.get('unit')))
            usgxml.append(xml_tag('currentValue',
                                  usage.get('currentValue')))
            regxml.append(usgxml)
        return regxml

    def does_vm_exists(self, vmname):
        vmname = vmname.split(".")
        #print(self.resources)
        for res in self.resources.values():
            for re in res:
                if type(re) == VirtualMachineInfo:
                    if re.computerName == vmname[0]:
                        print("VM Present")
                        return True
        return False

    def get_ipaddr_for_vm(self, vmname):
        vmname = vmname.split(".")
        for res in self.resources.values():
            for re in res:
                if type(re) == VirtualMachineInfo:
                    if re.computerName == vmname[0]:
                        print("Present")
                        return re.vmIP
        return None

class AzureResource:
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        self.name = name
        self.type = rtype
        self.location = location
        self.resourcegroup_name = rgname
        self.prop = {}
        self.properties_url = "{0}{1}".format(azure_mgmt_url, url_id)
        self.tenantName = tenantname

    def to_xml(self):
        res = xml.Element('resource')
        res.append(xml_tag('name', self.name))
        res.append(xml_tag('type', self.type))
        res.append(xml_tag('location', self.location))
        res.append(xml_tag('resourceGroupName', self.resourcegroup_name))
        return res

class StorageInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
        self.provisioningState = None
        self.performanceTier = None
    # get and parse function

    async def fetch(self, client, headers):
        storage_params = {'api-version': '2018-06-01'}
        atlogger.debug("StorageInfo fetch %s %s %s",client,headers,self.properties_url)
        self.properties_url=self.properties_url+'?api-version=2018-07-01'
        token=headers['Authorization']
        headers = {"Authorization": token}
        self.storageInfo=requests.get(self.properties_url,headers=headers).json()
        #self.storageInfo = await get_details_async(
        #    client, headers, self.properties_url, storage_params)
        atlogger.debug("StorageInfo is %s",self.storageInfo)

    async def parse(self):
        #props = self.storageInfo.get('properties', {})
        self.provisioningState = self.storageInfo['properties']['provisioningState']
        self.performanceTier = self.storageInfo['sku']['tier']

    def to_xml(self):
        res = super().to_xml()
        props = xml.Element('properties')
        res.append(props)
        props.append(xml_tag('provisioningState', self.provisioningState))
        props.append(xml_tag('performanceTier',self.performanceTier))
        return res

class VaultInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
        self.provisioningState = 'Succeeded'
        self.vaultInfo = None
    # get and parse function

    async def fetch(self, client, headers):
        vault_params = {'api-version': '2023-06-01'}
        atlogger.debug("VaultInfo fetch %s %s %s",client,headers,self.properties_url)
        self.properties_url=self.properties_url+'?api-version=2023-06-01'
        token=headers['Authorization']
        headers = {"Authorization": token}
        self.vaultInfo=requests.get(self.properties_url,headers=headers).json()
        #self.storageInfo = await get_details_async(
        #    client, headers, self.properties_url, storage_params)
        atlogger.debug("VaultInfo is %s",self.vaultInfo)

    async def parse(self):
        #props = self.storageInfo.get('properties', {})
        #atlogger.debug('@A vault prop: %s', self.vaultInfo)
        #self.provisioningState = self.vaultInfo['properties']['provisioningState']
        #atlogger.debug('@A vault state: %s', self.provisioningState)
        pass

    def to_xml(self):
        res = super().to_xml()
        props = xml.Element('properties')
        res.append(props)
        props.append(xml_tag('provisioningState', self.provisioningState))
        return res
# All classes can have method names Disk/Image/VM/Network and so forth.
class DiskInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
        self.disk_sizegb = None
        self.time_created = None
        self.provisioning_state = None
        self.disk_state = None

    async def fetch(self, client, headers):
        di_params = {'api-version': '2019-07-01'}
        self.diskInfo = await get_details_async(client, headers,
                                                self.properties_url, di_params)

    async def parse(self):
        props = self.diskInfo.get('properties', {})
        self.disk_sizegb = props.get('diskSizeGB', "")
        self.time_created = props.get('timeCreated', "")
        self.provisioning_state = props.get('provisioningState', "")
        self.disk_state = props.get('diskState', "")

    def to_xml(self):
        res = super().to_xml()
        properties = xml.Element('properties')
        res.append(properties)
        properties.append(xml_tag('diskSizeGB', self.disk_sizegb))
        properties.append(xml_tag('timeCreated', self.time_created))
        properties.append(
            xml_tag('provisioningState', self.provisioning_state))
        properties.append(xml_tag('diskState', self.disk_state))
        return res


class ImageInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
        # Image specific properites follows.
        self.provisioningState = None
        self.osDiskType = None
        self.osDiskSizeGB = None
        self.osState = None
        self.osDiskCaching = None
        self.osDiskStorageAccountType = None
        self.dataDiskSizeGB = None

    async def fetch(self, client, headers):
        im_params = {'api-version': '2019-12-01'}
        atlogger.debug("Image self.properties_url = {}".format(self.properties_url))
        self.imageInfo = await get_details_async(client, headers,
                                                 self.properties_url, im_params)

    async def parse(self):
        props = self.imageInfo.get('properties', {})
        storage_profile = props.get('storageProfile', {})
        #os_disk = props.get('osDisk', {})
        os_disk = storage_profile.get('osDisk', {})

        self.provisioningState = props.get('provisioningState', "")
        self.osDiskType = os_disk.get('osType', "")
        self.osDiskSizeGB = os_disk.get('diskSizeGB', "")
        self.osState = os_disk.get('osState', "")
        self.osDiskCaching = os_disk.get('caching', "")
        self.osDiskStorageAccountType = os_disk.get('storageAccountType', "")

        # FIXME:Am I doing correct calculation disk size?? More than one disk, how to handle???
        data_disks = storage_profile.get('dataDisks', [{}])
        if len(data_disks) > 0:
            self.dataDiskSizeGB = data_disks[0].get('dataDiskSizeGB')

    def to_xml(self):
        res = super().to_xml()
        properties = xml.Element('properties')
        res.append(properties)
        """
        <provisioningState>Succeeded</provisioningState>
        <osDiskType>Windows</osDiskType>
        <osDiskSizeGB>128</osDiskSizeGB>
        <osState>Generalized</osState>
        <osDiskCaching>ReadWrite</osDiskCaching>
        <osDiskStorageAccountType>StandardSSD_LRS</osDiskStorageAccountType>
        <dataDiskSizeGB/>
        """
        properties.append(xml_tag('provisioningState', self.provisioningState))
        properties.append(xml_tag('osDiskType', self.osDiskType))
        properties.append(xml_tag('osDiskSizeGB', self.osDiskSizeGB))
        properties.append(xml_tag('osState', self.osState))
        properties.append(xml_tag('osDiskCaching', self.osDiskCaching))
        properties.append(xml_tag('osDiskStorageAccountType',
                                  self.osDiskStorageAccountType))
        properties.append(xml_tag('dataDiskSizeGB', self.dataDiskSizeGB))
        return res


class NetworkInterfaceInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)

        # network interface specfic properties.
        self.provisioningState = None
        self.ipConfigName = None
        self.privateIPAddress = None
        self.privateIPAllocationMethod = None
        self.virtualNwName = None
        self.nsgName = None
        self.publicIPName = None
        self.nwInInfo = {}

    async def fetch(self, client, headers):
        nwi_params = {'api-version': '2020-05-01'}
        self.nwInInfo = await get_details_async(client, headers,
                                                self.properties_url, nwi_params)

    async def parse(self):
        # print(self.nwInInfo)
        props = self.nwInInfo.get('properties', {})
        ipconfigs = props.get('ipConfigurations', [{}])
        # FIXME: What if there are multiple ip configurations.
        ipconfig_props = ipconfigs[0].get('properties', {})
        self.provisioningState = props.get('provisioningState', "")
        self.ipConfigName = ipconfigs[0].get('name', "")
        self.privateIPAddress = ipconfig_props.get('privateIPAddress', "")
        self.privateIPAllocationMethod = ipconfig_props.get(
            'privateIPAllocationMethod', "")
        nsgid = props.get('networkSecurityGroup', {}).get('id', "")
        self.nsgName = nsgid.split('/')[-1]
        vnwid = ipconfig_props.get('subnet', {}).get('id', "")
        self.virtualNwName = vnwid.split('/')[-3]
        publicipid = ipconfig_props.get('publicIPAddress', {}).get('id', "")
        self.publicIPName = publicipid.split('/')[-1]

    def to_xml(self):
        res = super().to_xml()
        if self.privateIPAddress is not None and self.privateIPAllocationMethod is not None:
            props = xml.Element('properties')
            res.append(props)
            """
              <provisioningState>Succeeded</provisioningState>
              <ipConfigName>ipconfig1</ipConfigName>
              <privateIPAddress>10.125.1.10</privateIPAddress>
              <privateIPAllocationMethod>Static</privateIPAllocationMethod>
              <virtualNetworkName>NetwkName</virtualNetworkName>
            """
            props.append(xml_tag('provisioningState', self.provisioningState))
            props.append(xml_tag('ipConfigName', self.ipConfigName))
            props.append(xml_tag('privateIPAddress', self.privateIPAddress))
            props.append(xml_tag('privateIPAllocationMethod',self.privateIPAllocationMethod))
            props.append(xml_tag('virtualNetworkName', self.virtualNwName))
            return res
        return res


class VirtualNetworkInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
        # virutal network information.
        self.provisioningState = None
        self.addressPrefix = None
        self.subnetName = None
        self.subnetAddressPrefixes = None
        self.remoteAddressSpace = None
        self.peeringState = None
        self.vnwInfo = {}

    # Get and parse function.
    async def fetch(self, client, headers):
        vnw_params = {'api-version': '2020-05-01'}
        self.vnwInfo = await get_details_async(client, headers,
                                               self.properties_url, vnw_params)

    async def parse(self):
        atlogger.info("@VS Entering VirtualNetworkInfo parse")
        atlogger.debug("Network Properties:%s",self.vnwInfo)
        props = self.vnwInfo.get('properties', {})
        subnets = props.get('subnets', [])
        vnet_peerings = props.get('virtualNetworkPeerings', [{}])

        self.provisioningState = props.get('provisioningState', "")
        self.addressPrefix = props.get(
            'addressSpace', {}).get('addressPrefixes', [""])[0]
        subnet_names = [sub.get('name', "") for sub in subnets]
        subnet_prefixes = [sub.get("properties", "").get(
            "addressPrefix") for sub in subnets]

        self.subnetName = ",".join(subnet_names)
        self.subnetAddressPrefixes = ",".join(subnet_prefixes)
        atlogger.debug("Properties Details:%s", props)
        atlogger.debug("Subnet Details:%s", subnets)
        # FIXME: What about the possibility of multiple peering vnets.
        if len(vnet_peerings) > 0:
            self.remoteAddressSpace = vnet_peerings[0].get('properties', {}).get(
                'remoteAddressSpace', {}).get('addressPrefixes', '')
            self.peeringState = vnet_peerings[0].get(
                'properties', {}).get('peeringState', '')

    def to_xml(self):
        res = super().to_xml()
        props = xml.Element('properties')
        res.append(props)
        """
              <provisioningState>Succeeded</provisioningState>
              <addressPrefix>10.125.0.0/16</addressPrefix>
              <subnetName>LAN,DMZ</subnetName>
              <subnetAddressPrefixes>10.125.0.0/17,10.125.254.0/24</subnetAddressPrefixes>
              <remoteAddressSpace>none</remoteAddressSpace>
              <peeringState>none</peeringState>
        """
        props.append(xml_tag('provisioningState', self.provisioningState))
        props.append(xml_tag('addressPrefix', self.addressPrefix))
        props.append(xml_tag('subnetName', self.subnetName))
        props.append(xml_tag('subnetAddressPrefixes',
                             self.subnetAddressPrefixes))
        props.append(xml_tag('remoteAddressSpace', self.remoteAddressSpace))
        props.append(xml_tag('peeringState', self.peeringState))
        return res

class AvailabilitySetInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
        self.ipAddress = None
        self.hostSize = None
        self.interfaceName = None
        self.diskName = None
        self.hostPoolName = None

    async def fetch(self, client, headers):
        as_params = {'api-version': '2023-07-01'}
        self.availabilitySetInfo = await get_details_async(client, headers,
                                                self.properties_url, as_params)
        self.headers = headers
        atlogger.debug("@A Availabilityset: %s", self.availabilitySetInfo)

    async def parse(self):
        try:
            props = self.availabilitySetInfo.get('properties', {})
            vm = props.get('virtualMachines', {})
            resources = self.availabilitySetInfo.get('resources', {})
            if len(vm) > 0:
                hardwareProfile = vm.get('hardwareProfile', {})
                self.hostSize = hardwareProfile.get('vmSize', "")
                diskinfo = vm.get('osDisk', {})
                self.diskName = diskinfo.get('name', "")
                network = vm.get('networkInterfaces', {})
                self.netId = network.get('id', "")
                #self.fetch(self.client, self.headers, True)
                networkInt = request.get("https://management.azure.com" + str(self.netId), headers=self.headers)
                if networkInt.status_code == 200:
                    networkInt = networkInt.json()
                    self.interfaceName = networkInt.get("name", "")
                    net_props = networkInt.get("properties", {})
                    ipInfo = net_props.get("ipConfigurations", {})
                    ip_props = ipInfo.get("properties", {})
                    self.ipAddress = ip_props.get("privateIPAddress", "")

            if len(resources) > 0:
                for i in range(len(resources)):
                    if resources.get('name', "") == "Microsoft.PowerShell.DSC":
                        r_props = resources.get('properties', {})
                        r_sett = resources.get('settings', {})
                        r_set_props = r_sett.get('properties', {})
                        self.hostPoolName = r_set_props.get("hostPoolName", "")
        except Exception as e:
            atlogger.error("Error in AvailabilitySet parse: {}".format(e))
        
    def to_xml(self):
        res = super().to_xml()
        #atlogger.debug("@A xml in Availability")
        properties = xml.Element('properties')
        properties.append(xml_tag('ipAddress', self.ipAddress))
        properties.append(xml_tag('hostSize', self.hostSize))
        properties.append(xml_tag('interfaceName', self.interfaceName))
        properties.append(xml_tag('diskName', self.diskName))
        properties.append(xml_tag('hostPoolName', self.hostPoolName))
        res.append(properties)
        return res


class VirtualMachineInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)

        # Virtual machine specific properties.
        self.vmIP = None
        self.sku = None
        self.vmResourceId = None
        self.vmId = None
        self.vmSize = None
        self.osDiskName = None
        self.osType = None
        self.osCreateOption = None
        self.osCaching = None
        self.computerName = None
        self.powerState = None
        self.power_state = {'VM running': 'PowerOn',
                            'VM deallocated': 'PowerOff',
                            'VM stopped': 'PowerOff'}
        self.sessionType = None
        self.sess_type = {'19h2-evd': 'MultiSession',
                          '19h2-ent': 'SingleSession'}
        self.networkInterfaceName = None
        self.publicIPAddressName = None
        self.virtualNetworkName = None
        self.availabilitySetName = None
        self.networkSecurityGroupName = None
        self.provisioningState = None
        self.imageName = None
        self.vmInfo = {}
        self.vmAgentVersion = None
        self.vmAgentStatus = 'Inactive'
        self.vmRegistrationStatus = None
        self.cpu = None
        self.memory = None
        self.powerOn = None
        self.assignment = None
        self.hostPoolName = None
        self.vmHostingType = None
        #self.type = 'Azure'
        self.nw_data = None
        self.storageAccName = None
        #self.resources = None
        self.rid = url_id
        self.rgname = rgname
        self.tenantname = tenantname
        self.headers = None

    async def fetch(self, client, headers ):
        vm_params = {'api-version': '2019-12-01',
                     '$expand': 'instanceView'}
        self.headers = headers
        #self.client = client
        self.vmInfo = await get_details_async(client, headers,
                                          self.properties_url, vm_params)
        sizeUrl = self.properties_url + '/vmSizes'
        self.vmSizes = await get_details_async(client, headers,
                                              sizeUrl, vm_params)

        #Fetching vm up time report
        #vmUptime = VmUptime(self.rid,self.tenantname,headers,self.rgname)
        #self.nw_data = await self.get_vm_nwinter_info(client,headers,nw_interface_id)
    async def get_vm_nwinter_info(self, nw_url):
        atlogger.debug('inside Func')
        nwi_params = {'api-version': '2023-09-01'}
        nw_url = 'https://management.azure.com' + nw_url
        nw_data = requests.get(nw_url,headers = self.headers, params = nwi_params).json()
        return nw_data


    async def parse(self):
        atlogger.info("@VS Entering VirtualMachineInfo parse")
        props = self.vmInfo.get('properties', {})
        resources = self.vmInfo.get('resources', {})

        self.vmResourceId = self.vmInfo.get('id', '')
        vm_storage_prof = props.get('storageProfile', {})
        vm_network_prof = props.get('networkProfile', {})
        vm_os_disk = vm_storage_prof.get('osDisk', {})

        nw_interface_id = vm_network_prof.get('networkInterfaces', [{}])[0]\
            .get('id', "")
        networkDetails = await self.get_vm_nwinter_info(nw_interface_id)
        net_props = networkDetails.get('properties', {})
        net_ip = net_props.get('ipConfigurations', {})[0]
        net_ip_prop = net_ip.get('properties', {})
        self.vmIP = net_ip_prop.get('privateIPAddress', '')

        subnet = net_ip_prop.get('subnet', {})
        self.virtualNetworkName = subnet.get('id', '').split('/')[-3]
        self.sku = vm_storage_prof.get('imageReference', {}).get('sku', "")
        self.vmId = props.get('vmId', "")
        self.vmSize = props.get('hardwareProfile', {}).get('vmSize', "")

        self.osDiskName = vm_os_disk.get('name', "")
        self.osType = vm_os_disk.get('osType', "")
        self.osCreateOption = vm_os_disk.get('createOption', "")
        self.osCaching = vm_os_disk.get('caching', "")
        self.computerName = props.get('osProfile', {}).get('computerName', "")

        vmpower = props.get('instanceView', {}).get('statuses', "")
        state = [i['displayStatus']
                 for i in vmpower if "PowerState" in i['code']]
        self.powerState = self.power_state[state[0]]
        self.sessionType = self.sess_type.get(self.sku, "")
        self.provisioningState = props.get('provisioningState', "")

        # FIXME: From where these to be populated???
        self.networkInterfaceName = nw_interface_id.split('/')[-1]
        self.publicIPAddressName = None
        #self.virtualNetworkName = None
        avail_set_id = props.get('availabilitySet', {}).get('id', "")
        self.availabilitySetName = avail_set_id.split('/')[-1]
        storageAccountName = props.get("osDisk",{}).get("vhd",{}).get("uri","")
        atlogger.debug("@PG storageAccountName is %s",storageAccountName)
        if self.storageAccName:
            self.storageAccName = storageAccountName.split("/")[2].split(".")[0]
        else:
            self.storageAccName = None
        self.networkSecurityGroupName = None

        image_id = vm_storage_prof.get('imageReference', {}).get('id', "")
        self.imageName = image_id.split('/')[-1]
        
        self.vmRegistrationStatus = "Registered"
        identity = self.vmInfo.get("identity", {})
        try:
            self.assignment = identity.get("type", "")
            if self.assignment == 'SystemAssigned':
                self.assignment = 'Assigned'
            else:
                self.assignment = 'Not Assigned'
        except:
            self.assignment = 'Not Assigned'
        try:
            instanceView = props.get('instanceView', {})
            agentDetails = instanceView.get('vmAgent', {})
            self.vmAgentVersion = agentDetails.get('vmAgentVersion')
            agentDisplayStatus = False
            if agentDetails.get('statuses'):
                agentStatus = agentDetails.get('statuses')[0]
                agentDisplayStatus = agentStatus.get("displayStatus")
                atlogger.debug('@CD agentDisplayStatus : %s',agentDisplayStatus)
                self.powerOn = agentStatus.get("time")
            if agentDisplayStatus == "Ready":
                self.vmAgentStatus = "Active"
            else:
                self.vmAgentStatus = "Inactive"
        except Exception as e:
            #atlogger.debug(f"@PG Exception at agenversion {e}")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            atlogger.error("%s: @PG Exception at agentversion : %s %s %s",
                           e, exc_type, fname, str(exc_tb.tb_lineno))
            self.vmAgentVersion = None
            self.vmAgentStatus = 'Inactive'
        self.vmSizes = self.vmSizes["value"]
        for name in self.vmSizes:
            if name.get("name") == self.vmSize:
                self.cpu = name.get("numberOfCores")
                self.memory = name.get("memoryInMB")
        #self.client=None
        
        """vmidresourcemap_query = "INSERT INTO vmidresourcemap(vmid, resourceid) VALUES('{}', '{}')"
        vmidresourcemap_query = vmidresourcemap_query.format(self.vmId, self.vmResourceId)
        atlogger.info("vmidresourcemap_query = {}".format(vmidresourcemap_query))

        execute_query(vmidresourcemap_query)"""


    def to_xml(self):
        res = super().to_xml()
        props = xml.Element('properties')
        res.append(props)
        """
              <vmId>1cd757c5-7e31-4d21-86ea-ea09e73b8048</vmId>
              <vmIP>10.125.1.10</vmIP>
              <vmSize>Standard_B2ms</vmSize>
              <vmAgentState></vmAgentState>
	      <vmAgentVersion>22.2.0</vmAgentVersion>
    	      <vmRegistrationStatus>Registered</vmRegistrationStatus>
              <osDiskName>DC01osDisk</osDiskName>
              <osType>Windows</osType>
              <osCreateOption>Attach</osCreateOption>
              <osCaching>ReadWrite</osCaching>
              <computerName/>
              <powerState>PowerOff</powerState>
              <sku/>
              <sessionType/>
              <networkInterfaceName>dc01nic</networkInterfaceName>
              <publicIPAddressName>DC01PiP</publicIPAddressName>
              <virtualNetworkName>NerdioVnet</virtualNetworkName>
              <availabilitySetName/>
              <networkSecurityGroupName/>
              <imageName/>
              <provisioningState>Succeeded</provisioningState>
        """
        self.type_value = 'azure'
        props.append(xml_tag('vmId', self.vmId))
        props.append(xml_tag('vmIP', self.vmIP))
        props.append(xml_tag('vmSize', self.vmSize))
        props.append(xml_tag('vmAgentState', self.vmAgentStatus))
        props.append(xml_tag('vmAgentVersion', self.vmAgentVersion))
        props.append(xml_tag('vmRegistrationStatus', self.vmRegistrationStatus))
        props.append(xml_tag('cpu', self.cpu))
        props.append(xml_tag('memorySize', self.memory))
        props.append(xml_tag('powerOn', self.powerOn))
        props.append(xml_tag('vmHostingType', self.vmHostingType))
        props.append(xml_tag('type', self.type_value))
        props.append(xml_tag('vmAssignment', self.assignment))
        props.append(xml_tag('osDiskName', self.osDiskName))
        props.append(xml_tag('osType', self.osType))
        #props.append(xml_tag('osCreateOption', self.osCreateOption))
        props.append(xml_tag('osDiskCreateOption', self.osCreateOption))
        props.append(xml_tag('osCaching', self.osCaching))
        props.append(xml_tag('computerName', self.computerName))
        props.append(xml_tag('powerState', self.powerState))
        props.append(xml_tag('sku', self.sku))
        props.append(xml_tag('sessionType', self.sessionType))
        props.append(xml_tag('networkInterfaceName',
                             self.networkInterfaceName))
        props.append(xml_tag('publicIPAddressName', self.publicIPAddressName))
        props.append(xml_tag('virtualNetworkName', self.virtualNetworkName))
        props.append(xml_tag('availabilitySet', self.availabilitySetName))
        props.append(xml_tag('storageAccountName', self.storageAccName))
        props.append(xml_tag('networkSecurityGroupName',
                             self.networkSecurityGroupName))
        props.append(xml_tag('imageName', self.imageName))
        props.append(xml_tag('provisioningState', self.provisioningState))
        return res


class PublicIPAddressInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
        self.provisioningState = None
        self.privateIPAddress = None
        self.resourceGuid = None
        self.ipaddress = None
        self.publicIPAddressVersion = None
        self.publicIPAllocationMethod = None
        self.idleTimeoutInMinutes = None
        self.publicIPInfo = {}

    # Get and parse funtion
    async def fetch(self, client, headers):
        pia_params = {'api-version': '2020-05-01'}
        self.publicIPInfo = await get_details_async(client, headers,
                                                    self.properties_url, pia_params)

    async def parse(self):
        atlogger.info("@VS Entering PublicIPAddressInfo parse")
        props = self.publicIPInfo.get('properties', {})
        self.provisioningState = props.get('provisioningState', "")
        self.privateIPAddress = ""  # TODO
        self.resourceGuid = props.get('resourceGuid', "")
        self.ipaddress = props.get('ipAddress', "")
        self.publicIPAddressVersion = props.get('publicIPAddressVersion', "")
        self.publicIPAllocationMethod = props.get(
            'publicIPAllocationMethod', "")
        self.idleTimeoutInMinutes = props.get('idleTimeoutInMinutes', "")

    def to_xml(self):
        res = super().to_xml()
        props = xml.Element('properties')
        res.append(props)
        """
              <provisioningState>Succeeded</provisioningState>
              <resourceGuid>f9418698-f5c7-4241-8b58-e54d913aeef3</resourceGuid>
              <ipaddress>13.65.209.164</ipaddress>
              <publicIPAddressVersion>IPv4</publicIPAddressVersion>
              <publicIPAllocationMethod>Static</publicIPAllocationMethod>
              <idleTimeoutInMinutes>4</idleTimeoutInMinutes>
              <privateIPAddress>10.125.1.10</privateIPAddress>
        """
        props.append(xml_tag('provisioningState', self.provisioningState))
        props.append(xml_tag('resourceGuid', self.resourceGuid))
        props.append(xml_tag('ipaddress', self.ipaddress))
        props.append(xml_tag('publicIPAddressVersion',
                             self.publicIPAddressVersion))
        props.append(xml_tag('publicIPAllocationMethod',
                             self.publicIPAllocationMethod))
        props.append(xml_tag('idleTimeoutInMinutes',
                             self.idleTimeoutInMinutes))
        props.append(xml_tag('privateIPAddress', self.privateIPAddress))
        return res


class VPNInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
        self.id = None
        self.location = None
        self.provisioningState = None
        self.vpngateway = None
        self.localgateway = None
        self.connectionType = None
        self.connectionProtocol = None
        self.routingWeight = None
        self.dpdTimeoutSeconds = None
        self.enableBgp = None
        self.useLocalAzureIpAddress = None
        self.usePolicyBasedTrafficSelectors = None
        self.connectionStatus = None
        self.ingressBytesTransferred = None
        self.egressBytesTransferred = None
        self.vpnInfo = {}
        self.tenantName = tenantname
        self.start_time = None
        self.end_time = None
        self.sourceIp = None
        self.destinationIp = None
        self.report_dicts = {}
        self.headers = None
    
    async def sql_executor(self, query):
        try:
            conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='postgres', database='anuntatech')
            curObj = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            atlogger.debug(f"@HJ Executing --> '{query}'")
            curObj.execute(query)
            conn.commit()
            curObj.close()
            conn.close()
            return True

        except Exception as e:
            atlogger.debug(f"@HJ Caught Execption '{e}' while executing {query}")
            return False


    # Get and parse function
    async def fetch(self, client, headers):
        atlogger.debug("@A inside VPNInfo Fetch method")
        vpn_params = {'api-version': '2018-06-01'}
        self.vpnInfo = await get_details_async(
            client, headers, self.properties_url, vpn_params)
        netflow_params = {"api-version": "2021-05-01",
                        "interval": "PT5M",
                        "metricnames": "BitsOutPerSecond,BitsInPerSecond"
                        }
        netflow_url = "{}/providers/Microsoft.Insights/metrics"
        netflow_url = netflow_url.format(self.properties_url)
        netflow_resp = requests.get(netflow_url, headers=headers, params=netflow_params)
        self.netflow = {}
        self.headers = headers

        if netflow_resp.status_code == 200:
            netflow_resp = netflow_resp.json()
            metrics = netflow_resp["value"]
            atlogger.debug(f"@CD Metrics len =  {len(metrics)}")
            for i in range(len(metrics)):
                atlogger.debug(f"@CD metrics i = {i}")
                if metrics[i]["name"]["value"] == "BitsInPerSecond":
                    try:
                        atlogger.debug("@A, data %s %s", metrics[i]["timeseries"][0]["data"], len(metrics[i]["timeseries"][0]["data"]))
                        for j in range(len( metrics[i]["timeseries"][0]["data"])//2):
                            atlogger.debug("@A ingress ingress")
                            time = metrics[i]["timeseries"][0]["data"][j]["timeStamp"]
                            byte = int(metrics[i]["timeseries"][0]["data"][j]["average"]) / 8
                            if time not in self.report_dicts:
                                self.report_dicts[time] = {"ingress": 0, "egress": 0}
                            self.report_dicts[time]["ingress"] = byte
                            """self.end_time =  metrics[i]["timeseries"][0]["data"][i+1]["timeStamp"]
                            self.netflow["ingressbytes"] = metrics[i]["timeseries"][0]["data"][i]["average"]
                            self.netflow["ingressbytes"] = int(self.netflow["ingressbytes"]) /8"""
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        atlogger.error("%s: @A ingress: %s %s %s",
                           e, exc_type, fname, str(exc_tb.tb_lineno))
                        self.netflow["ingressbytes"] = 0
                if metrics[i]["name"]["value"] == "BitsOutPerSecond":
                    try:
                        for j in range(len(metrics[i]["timeseries"][0]["data"])//2):
                            atlogger.debug("@A egress inside")
                            time = metrics[i]["timeseries"][0]["data"][j]["timeStamp"]
                            byte = int(metrics[i]["timeseries"][0]["data"][j]["average"]) / 8
                            if time not in self.report_dicts:
                                self.report_dicts[time] = {"ingress": 0, "egress": 0}
                            self.report_dicts[time]["egress"] = byte
                            """self.netflow["egressbytes"] = metrics[i]["timeseries"][0]["data"][1]["average"]
                            self.netflow["egressbytes"] = int(self.netflow["egressbytes"]) /8"""
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        atlogger.error("%s: @A egress : %s %s %s",
                           e, exc_type, fname, str(exc_tb.tb_lineno))
                        self.netflow["egressbytes"] = 0
            atlogger.debug("@A vpn bytes: %s", self.report_dicts)
        
           
        else:
            print("Error in API {} {}".format(netflow_url, netflow_resp.text))

    async def parse(self):
        atlogger.debug("@HJ inside VPNInfo Parse method")
        self.id = self.vpnInfo.get('id', '')
        self.location = self.vpnInfo.get('location', '')

        props = self.vpnInfo.get('properties', {})
        self.provisioningState = props.get('provisioningState', '')
        try:
            #if props.get('provisioningState', '') == 'IPsec'
            self.vpngateway = props.get('virtualNetworkGateway1', {}).get('id', "")
            self.localgateway = props.get('localNetworkGateway2', {}).get('id', "")
            atlogger.debug("@A self.vpngateway: %s", self.vpngateway)
            atlogger.debug("@A self.localgateway: %s", self.localgateway)
            vpn_params = {"api-version": "2023-02-01"}
            atlogger.debug("@A headers: %s", self.headers)
            url = "http://management.azure.com" + str(self.vpngateway)
            atlogger.debug("@A url: %s", url)
            vpngateway_resp_code = requests.get("https://management.azure.com" + str(self.vpngateway), headers=self.headers, params=vpn_params)

            atlogger.debug("@A status code: %s", vpngateway_resp_code.status_code)
            if vpngateway_resp_code.status_code == 200:
                vpngateway_resp = vpngateway_resp_code.json()

            localgateway_resp = requests.get("https://management.azure.com" + str(self.localgateway), headers=self.headers, params=vpn_params).json()
            
            atlogger.debug("@A vpngateway_resp: %s", vpngateway_resp)
            atlogger.debug("@A localgateway_resp: %s", localgateway_resp)

            ip_conf_vpngateway = vpngateway_resp.get('properties', {}).get('ipConfigurations', {})[0]

            if ip_conf_vpngateway:
                public_vpn = ip_conf_vpngateway.get('properties', {}).get('publicIPAddress', {})
                public_vpn_url =  public_vpn.get('id', "")
                atlogger.debug("@A public_resp id: %s", public_vpn_url)
                public_vpn_resp = requests.get("https://management.azure.com" + str(public_vpn_url), headers=self.headers, params=vpn_params).json()
                self.sourceIp = public_vpn_resp.get('properties', {}).get('ipAddress', "")
                atlogger.debug("@A destinationIp: %s",self.sourceIp) 

            ip_conf_localgateway = localgateway_resp.get('properties', {}).get('gatewayIpAddress', "")

            if ip_conf_localgateway != "":
                self.destinationIp = ip_conf_localgateway
                atlogger.debug("@A destinationIp: %s",self.destinationIp)
        except Exception as e:
            atlogger.error("@A Exception in Vpn: %s", e)

        self.connectionType = props.get('connectionType', "")
        self.connectionProtocol = props.get('connectionProtocol', "")
        self.routingWeight = props.get('routingWeight', "")
        self.dpdTimeoutSeconds = props.get('dpdTimeoutSeconds', "")
        self.enableBgp = props.get('enableBgp', "")
        self.useLocalAzureIpAddress = props.get('useLocalAzureIpAddress', "")
        self.usePolicyBasedTrafficSelectors = props.get(
            'useLocalAzureIpAddress', "")
        self.connectionStatus = props.get('connectionStatus', "")
        self.ingressBytesTransferred = props.get('ingressBytesTransferred', "")
        self.egressBytesTransferred = props.get('egressBytesTransferred', "")
        timeCreated=get_current_time()
        dt_object = datetime.strptime(timeCreated, "%Y-%m-%dT%H:%M:%SZ")
        startTime = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
        startTime = startTime.strftime("%Y-%m-%d %H:%M:%S.%f")
       
        try:
            for key,value in self.report_dicts.items():
                end_time = datetime.strptime(key, "%Y-%m-%dT%H:%M:%SZ")
                end_time = end_time + timedelta(minutes=5)
                query = f"insert into vpnutilization(resourcepoolid, vpnname, ingress, egress, start_time, end_time) values('{self.tenantName}', '{self.name}', '{value['ingress']}', '{value['egress']}','{key}' , '{end_time}')"
                atlogger.debug(f'@A Entering vpn with query {query}')
                check_flag = await self.sql_executor(query)
                if check_flag:
                    atlogger.debug("Insert query successfull")
                else:
                    atlogger.error("Error in insert query")
        except Exception as e:
            atlogger.debug("@A vpnExcept %s", e)


    def to_xml(self):
        res = super().to_xml()
        props = xml.Element('properties')
        res.append(props)
        """
              <id>/subscriptions/b3e33af0-f770-44b5-82cc-d75aeb75a7d7/resourceGroups/LocalNetworkGateway-RG/providers/Microsoft.Network/connections/vpn_siteto_site</id>
              <location>eastus2</location>
              <provisioningState>Succeeded</provisioningState>
              <vpngateway>/subscriptions/b3e33af0-f770-44b5-82cc-d75aeb75a7d7/resourceGroups/AnuntaWVDVVtest/providers/Microsoft.Network/virtualNetworkGateways/WVD_VPN</vpngateway>
              <localgateway>/subscriptions/b3e33af0-f770-44b5-82cc-d75aeb75a7d7/resourceGroups/LocalNetworkGateway-RG/providers/Microsoft.Network/localNetworkGateways/LNG_to_RedZone</localgateway>
              <connectionType>IPsec</connectionType>
              <connectionProtocol>IKEv1</connectionProtocol>
              <routingWeight>0</routingWeight>
              <dpdTimeoutSeconds>0</dpdTimeoutSeconds>
              <enableBgp>False</enableBgp>
              <useLocalAzureIpAddress>False</useLocalAzureIpAddress>
              <usePolicyBasedTrafficSelectors>False</usePolicyBasedTrafficSelectors>
              <connectionStatus>Connecting</connectionStatus>
              <ingressBytesTransferred>0</ingressBytesTransferred>
              <egressBytesTransferred>0</egressBytesTransferred>
        """
        props.append(xml_tag('id', self.id))
        props.append(xml_tag('location', self.location))
        props.append(xml_tag('provisioningState', self.provisioningState))
        props.append(xml_tag('vpngateway', self.vpngateway))
        props.append(xml_tag('localgateway', self.localgateway))
        props.append(xml_tag('connectionType', self.connectionType))
        props.append(xml_tag('connectionProtocol', self.connectionProtocol))
        props.append(xml_tag('routingWeight', self.routingWeight))
        props.append(xml_tag('dpdTimeoutSeconds', self.dpdTimeoutSeconds))
        props.append(xml_tag('enableBgp', self.enableBgp))
        props.append(xml_tag('useLocalAzureIpAddress',
                             self.useLocalAzureIpAddress))
        props.append(xml_tag('usePolicyBasedTrafficSelectors',
                             self.usePolicyBasedTrafficSelectors))
        props.append(xml_tag('connectionStatus', self.connectionStatus))
        props.append(xml_tag('ingressBytesTransferred',
                             self.ingressBytesTransferred))
        props.append(xml_tag('egressBytesTransferred',
                             self.egressBytesTransferred))
        props.append(xml_tag('sourceIp', self.sourceIp))
        props.append(xml_tag('destinationIp', self.destinationIp))
        return res


class NSGInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
        self.securityRules = None
        self.defaultSecurityRules = None
        self.nsgInfo = {}

    # get and parse function
    async def fetch(self, client, headers):
        nsg_params = {'api-version': '2020-05-01'}
        self.nsgInfo = await get_details_async(client, headers,
                                               self.properties_url, nsg_params)

    async def parse(self):
        props = self.nsgInfo.get('properties', {})
        self.securityRules = props.get('securityRules', [])
        # print(self.securityRules)
        self.defaultSecurityRules = props.get('defaultSecurityRules', [])

    def to_xml(self):
        res = super().to_xml()
        props = xml.Element('properties')
        res.append(props)
        rules = xml.Element('securityRules')
        props.append(rules)
        # FIXME: Response does not distinguish between secuirtyRules and defaultSecurityRules.
        # print(self.securityRules)
        if self.securityRules != None:
            for r in self.securityRules:
                rules.append(NSGRule(r).to_xml())
            for r in self.defaultSecurityRules:
                rules.append(NSGRule(r).to_xml())
        return res
        """
            <securityRules>
                <rule>
                  <name>RDP</name>
                  <protocol>TCP</protocol>
                  <sourcePortRange>*</sourcePortRange>
                  <destinationPortRange>3389</destinationPortRange>
                  <sourceAddressPrefix>*</sourceAddressPrefix>
                  <destinationAddressPrefix>*</destinationAddressPrefix>
                  <access>Allow</access>
                  <priority>300</priority>
                  <direction>Inbound</direction>
                </rule>
            </securityRules>
        """


class NSGRule:
    def __init__(self, info):
        self.name = info.get('name', "")
        props = info.get('properties', {})
        self.protocol = props.get('protocol', "")
        self.sourcePortRange = props.get('sourcePortRange', "")
        self.destinationPortRange = props.get('destinationPortRange', "")
        self.sourceAddressPrefix = props.get('sourceAddressPrefix', "")
        self.destinationAddressPrefix = props.get(
            'destinationAddressPrefix', "")
        self.access = props.get('access', "")
        self.priority = props.get('priority', "")
        self.direction = props.get('direction', "")

    def to_xml(self):
        rule = xml.Element('rule')
        rule.append(xml_tag('name', self.name))
        rule.append(xml_tag('protocol', self.protocol))
        rule.append(xml_tag('sourcePortRange', self.sourcePortRange))
        rule.append(xml_tag('destinationPortRange', self.destinationPortRange))
        rule.append(xml_tag('sourceAddressPrefix', self.sourceAddressPrefix))
        rule.append(xml_tag('destinationAddressPrefix',
                            self.destinationAddressPrefix))
        rule.append(xml_tag('access', self.access))
        rule.append(xml_tag('priority', self.priority))
        rule.append(xml_tag('direction', self.direction))
        return rule


class DefaultTypeInfo(AzureResource):
    def __init__(self, url_id, azure_mgmt_url,
                 name, rtype, location, rgname, tenantname):
        super().__init__(url_id, azure_mgmt_url,
                         name, rtype, location, rgname, tenantname)
    # get and parse function

    async def fetch(self, client, headers):
        pass

    async def parse(self):
        pass

    def to_xml(self):
        res = super().to_xml()
        props = xml.Element('properties')
        res.append(props)
        return res


class ADSyncData:
    def __init__(self, creds):
        self.creds = creds
        self.OnPremisesSyncEnabled = None
        self.lastSyncStatus = None
        self.OnPremisesLastSyncDateTime = None
        self.directoryQuota = None
        self.usedQuota = None
        self.totalQuota = None

    async def fetch(self, headers):
        try:
            async with aiohttp.ClientSession(headers=headers,
                                             trust_env=True) as connection:
                sync_url = 'https://graph.microsoft.com/v1.0/organization'
                adsync_data = await get_details_async(connection, headers,
                                                      sync_url)
                return adsync_data
        except Exception as e:
            print(str(e))

    async def parse(self, ad_data):
        self.OnPremisesSyncEnabled = ad_data.get('onPremisesSyncEnabled')
        if self.OnPremisesSyncEnabled == None or\
                self.OnPremisesSyncEnabled == '':
            self.OnPremisesSyncEnabled = 'False'
        self.OnPremisesLastSyncDateTime = ad_data.get(
            'onPremisesLastSyncDateTime')
        #current_time = datetime.now()
        current_time_tz = get_current_time()
        current_time_delta = convert_date_to_dateobject(current_time_tz)
        sync_time_delta = convert_date_to_dateobject(
            self.OnPremisesLastSyncDateTime)
        time_difference = current_time_delta - sync_time_delta
        mins = time_difference.total_seconds()/60
        if mins < 360:
            self.lastSyncStatus = 'SUCCESS'
        else:
            self.lastSyncStatus = 'FAILURE'
        self.usedQuota = ad_data.get('directorySizeQuota').get('used')
        self.totalQuota = ad_data.get('directorySizeQuota').get('total')

    def to_xml(self, adxml):
        #adxml = xml.Element('ADSync')
        adxml.append(xml_tag('enabled', self.OnPremisesSyncEnabled))
        adxml.append(xml_tag('lastSyncStatus', self.lastSyncStatus))
        adxml.append(xml_tag('lastSyncTime',
                             self.OnPremisesLastSyncDateTime))
        dir_qut = xml.Element('directoryQuota')
        dir_qut.append(xml_tag('used', self.usedQuota))
        dir_qut.append(xml_tag('total', self.totalQuota))
        adxml.append(dir_qut)
        return adxml


class WVDDetails:
    def __init__(self, tenantname, sub_id, creds):
        self.creds = creds
        self.tntname = tenantname
        atlogger.debug("@PG self.tntname in WVDDetails is %s", self.tntname)
        self.sub_id = sub_id
        # List of tenants
        self.tenants = []
        # List of hostpool objects includes fall,spring
        self.hostpools = []
        # List of sessionhost objects
        self.sessionhosts = []
        # List of appgroup objects
        self.appgroups = []
        # List of remote desktop objects
        self.remotedesktops = []
        # List of remote app objects
        self.remoteapps = []
        # List of user provisioned objects
        self.userassignments = []
        self.workspaces = []
        self.application_and_package_info = None
        self.licenses = []
        self.userAllocError = []

    async def fetch(self):
        try:
            # TODO:Move to  utilites
            atlogger.debug("Going to fetch wvd details")
            fall_mgmt_url = 'https://mrs-prod.ame.gbl/mrs-RDInfra-prod'
            fall_token = acquire_authtoken(self.creds.tenantid, self.creds.
                                           applicationid, self.creds.appsecret, fall_mgmt_url)
            fall_headers = form_auth_headers(fall_token)
            # Spring
            spring_mgmt_url = 'https://management.azure.com'
            spring_token = acquire_authtoken(self.creds.tenantid, self.creds.
                                             applicationid, self.creds.appsecret, spring_mgmt_url)
            spring_headers = form_auth_headers(spring_token)

            self.tenants = await self.fetch_tenant_details(fall_headers)

            ten_names = []
            if self.tenants != []:
               ten_names = [ten['tenantName'] for ten in self.tenants]
            print(ten_names)

            self.hostpools = await self.fetch_hostpool_details(fall_headers,
                                                               spring_headers, ten_names, self.sub_id)
            print("Hotspool Objects")
            print(self.hostpools)
            self.sessionhosts = await self.fetch_sessionhost_details(fall_headers, spring_headers, self.hostpools)
            print("Session host Objects")
            print(self.sessionhosts)
            self.appgroups = await self.fetch_appgroup_details(fall_headers,
                                                               spring_headers, self.hostpools, self.sub_id)
            print("AppGroup Objects")
            print(self.appgroups)
            self.remotedesktops = await self.fetch_remdes_details(fall_headers,
                                                                  spring_headers, self.appgroups)
            print("Desktop objects")
            print(self.remotedesktops)
            self.remoteapps = await self.fetch_remapp_details(fall_headers,
                                                              spring_headers, self.appgroups)
            self.userassignments = await self.fetch_useraccess_details(fall_headers, spring_headers, self.sub_id)
            print("User Prov Objects")
            print(self.userassignments)
            self.workspaces = await self.fetch_workspace_details(spring_headers, self.sub_id)
            print("WorkSpace Objects")
            print(self.workspaces)

            self.application_and_package_info = await self.fetch_application_and_package_details(self.sub_id, self.hostpools, self.appgroups, self.sessionhosts)
            
            self.licenses = await self.fetch_license_details(spring_headers)
            print("License Objects")
            print(self.licenses)

            self.userAllocError = await self.fetch_user_alloc_err_details(spring_headers, self.sub_id)
            print("User Allocation Error Objects")
            print(self.userAllocError)

        except Exception as e:
            print(str(e))

    async def fetch_tenant_details(self, headers):
        async with aiohttp.ClientSession(headers=headers, trust_env=True) as session:
            fall_base_url = 'https://rdbroker.wvd.microsoft.com/RdsManagement'\
                '/V1/TenantGroups/Default%20Tenant%20Group'
            tenant = TenantDetails()
            await tenant.fetch(session, headers, fall_base_url)
            self.tenants = await tenant.parse()
            if self.tenants == None:
               self.tenants = []
            return self.tenants

    async def fetch_hostpool_details(self, fall_headers,
                                     spring_headers, ten_names, sub_id):
        try:
            print("@PG Going to fetch hostpool details %s", ten_names)
            hstpool = HostPoolDetails(self.tntname,self.creds)
            hostpool_list_v1, hostpool_list_v2, maintenanceGrp = await hstpool.fetch(fall_headers, spring_headers, ten_names, sub_id)
            self.hostpools = await hstpool.parse(hostpool_list_v1,
                                                 hostpool_list_v2, maintenanceGrp, spring_headers)
            return self.hostpools
        except Exception as e:
            atlogger.error("Exception occurred in fetch hostpool details {}".format(e))
            return []

    async def fetch_sessionhost_details(self, fall_headers, spring_headers,
                                        hostpools):
        try:
            atlogger.debug("Fetching sessionhost details")
            sesshost = SessionHostDetails(self.tntname)
            sesshost_list_v1, sesshost_list_v2 = await sesshost.fetch(fall_headers, spring_headers, hostpools)
            self.sessionhosts = await sesshost.parse(sesshost_list_v1,
                                                     sesshost_list_v2, spring_headers)
            return self.sessionhosts
        except Exception as e:
            print(str(e))

    async def fetch_appgroup_details(self, fall_headers, spring_headers,
                                     hostpools, sub_id):
        try:
            atlogger.debug("Going to fetch AppGroup details")
            appgrp = AppGroupDetails()
            appgrp_list_v1, appgrp_list_v2 = await appgrp.fetch(fall_headers,
                                                                spring_headers, hostpools, sub_id)
            self.appgroups = await appgrp.parse(appgrp_list_v1,
                                                appgrp_list_v2)
            return self.appgroups
        except Exception as e:
            print(str(e))
            return []

    async def fetch_remapp_details(self, fall_headers, spring_headers,
                                   appgroups):
        try:
            atlogger.debug("Going to fetch remote app details")
            remapp = RemoteAppDetails()
            remapp_list_v1, remapp_list_v2 = await remapp.fetch(fall_headers,
                                                                spring_headers, appgroups)
            self.remoteapps = await remapp.parse(remapp_list_v1,
                                                 remapp_list_v2)
            return self.remoteapps
        except Exception as e:
            atlogger.error("Exception occured in remoteapp details")
            return []

    async def fetch_remdes_details(self, fall_headers, spring_headers,
                                   appgroups):
        try:
            atlogger.debug("Going to fetch remote desktop details")
            remdes = RemoteDesktopDetails()
            remdes_list_v1, remdes_list_v2 = await remdes.fetch(fall_headers,
                                                                spring_headers, appgroups)
            self.remotedesktops = await remdes.parse(remdes_list_v1,
                                                     remdes_list_v2, appgroups)
            return self.remotedesktops
        except Exception as e:
            print(str(e))
            return []

    async def fetch_useraccess_details(self, fall_headers, spring_headers,
                                       sub_id):
        try:
            atlogger.debug("Going to fetch users provisioned")
            graph_mgmt_url = 'https://graph.microsoft.com'
            graph_token = acquire_authtoken(self.creds.tenantid, self.creds.
                                            applicationid, self.creds.appsecret, graph_mgmt_url)
            graph_headers = form_auth_headers(graph_token)
            atlogger.debug("@PG graph_headers fetched")
            usrassg = UserProvisionedDetails()
            user_list_v1, user_list_v2, ad_users = await usrassg.fetch(fall_headers, spring_headers, graph_headers, sub_id,
                                                                       self.appgroups)
            atlogger.debug("@PG user list fetched successfully")
            self.userassignments = await usrassg.parse(user_list_v1, user_list_v2,
                                                       ad_users,self.appgroups, graph_headers)
            atlogger.debug("@PG going to return self.userassignments")
            return self.userassignments
        except Exception as e:
            atlogger.debug("@PG exception as %s", e)
            return []

    async def fetch_workspace_details(self, spring_headers, sub_id):
        try:
            atlogger.debug("GOing to fetch workspace details")
            wrkspc = WorkSpaceDetails()
            wrkspc_list = await wrkspc.fetch(spring_headers, sub_id)
            for wrks in wrkspc_list:
                wrkspc = WorkSpaceDetails()
                await wrkspc.parse(wrks)
                self.workspaces.append(wrkspc)

            return self.workspaces
        except Exception as e:
            print(str(e))
            return []

    async def fetch_application_and_package_details(self, subscription_id, hostpools, applicationgroups, sessionhosts):
        try:
            atlogger.debug("@VS Going to fetch application and package details")
            graph_url = 'https://graph.microsoft.com'
            management_url = 'https://management.azure.com'
            log_url = 'https://westus2.api.loganalytics.io'
            graph_bearer_token = acquire_authtoken(self.creds.tenantid, self.creds.applicationid, self.creds.appsecret, graph_url)
            management_bearer_token = acquire_authtoken(self.creds.tenantid, self.creds.applicationid, self.creds.appsecret, management_url)
            log_bearer_token = acquire_authtoken(self.creds.tenantid, self.creds.applicationid, self.creds.appsecret, log_url)
            
            graph_bearer_token = graph_bearer_token["accessToken"]
            management_bearer_token = management_bearer_token["accessToken"]
            log_bearer_token = log_bearer_token["accessToken"]
            
            application_and_package_info = ApplicationAndPackageInfo(self.tntname, subscription_id, management_bearer_token, graph_bearer_token, log_bearer_token, hostpools, applicationgroups, sessionhosts)
            await application_and_package_info.fetch_application_and_package_info()


            atlogger.debug("@VS Completing fetch application and package details")

            atlogger.info("Starting Disk Report")
            
            await self.fetch_disk_report(log_bearer_token, subscription_id, sessionhosts)
    
            atlogger.info("Completed Disk Report")

            return application_and_package_info
        except Exception as e:
            atlogger.error("@VS Error occurend in fetchin application and package details : {}".format(str(e)))
            return []    

    async def fetch_disk_report(self, log_bearer_token, subscription_id, sessionhosts):
        try:
            vm_details_map = {}
            for sh in sessionhosts:
                shname = sh.vmName.lower()
                if shname not in vm_details_map.keys():
                    vm_details_map[shname] = {}
                vm_details_map[shname]["ip"] = sh.ipAddress
                vm_details_map[shname]["hp"] = sh.hostPoolName

            atlogger.debug(vm_details_map)
    
            log_headers = {"Authorization": "Bearer "+log_bearer_token}
            query = """
            InsightsMetrics
            | where Origin == 'vm.azm.ms'
            | where Namespace == 'LogicalDisk'
            | where Name == 'FreeSpaceMB'
            | summarize by Tags, Computer, Name, Val
            | distinct Tags, Computer, Val
            """

            body = {"query": query}
            report = {}
            report_rows = []
            url = "https://api.loganalytics.io/v1/subscriptions/{}/query".format(subscription_id)
            res = requests.post(url, json=body , headers=log_headers)
            for table in res.json()["tables"]:
                rows = table["rows"]
                for row in rows:
                    props = json.loads(row[0])
                    vm = row[1]
                    avail = row[2]
                    if vm not in report.keys():
                        report[vm] = {}
                    if props["vm.azm.ms/mountId"] not in report[vm].keys():
                        report[vm][props["vm.azm.ms/mountId"]] = {}
                        report[vm][props["vm.azm.ms/mountId"]]["total"] = props["vm.azm.ms/diskSizeMB"]/1024
                    report[vm][props["vm.azm.ms/mountId"]]["used"] = report[vm][props["vm.azm.ms/mountId"]]["total"] - avail/1024
    
            for vm in report.keys():
                sum_total = 0
                sum_used = 0
                for disk in report[vm].keys():
                    sum_total += report[vm][disk]["total"]
                    sum_used += report[vm][disk]["used"]

                timeCreated=get_current_time()
                dt_object = datetime.strptime(timeCreated, "%Y-%m-%dT%H:%M:%SZ")
                startTime = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
                startTime = startTime.strftime("%Y-%m-%d %H:%M:%S.%f")
                atlogger.debug(vm.lower())
                atlogger.debug(vm_details_map[vm.lower()]["ip"])

                report_rows.append([self.tntname, vm_details_map[vm.lower()]["hp"], vm, sum_total, sum_used, startTime, timeCreated, vm_details_map[vm.lower()]["ip"]])

            for i in report_rows:
                insert_query = "insert into diskcapacityusage(resourcepoolid, poolname, servername, total, used, start_time, end_time, serverip) values('{}', '{}', '{}', {}, {}, '{}', '{}', '{}')"
                insert_query = insert_query.format(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7])
                execute_query(insert_query)
                atlogger.info(i)
            insights_enabled = [row[2].lower() for row in report_rows]
            for vm in vm_details_map.keys():
                timeCreated=get_current_time()
                dt_object = datetime.strptime(timeCreated, "%Y-%m-%dT%H:%M:%SZ")
                startTime = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
                startTime = startTime.strftime("%Y-%m-%d %H:%M:%S.%f")
                if vm.lower() not in insights_enabled:
                    insert_query = "insert into diskcapacityusage(resourcepoolid, poolname, servername, total, used, start_time, end_time, serverip) values('{}', '{}', '{}', {}, {}, '{}', '{}', '{}')"
                    insert_query = insert_query.format(self.tntname, vm_details_map[vm.lower()]["hp"], vm, 0, 0,  startTime, timeCreated, vm_details_map[vm.lower()]["ip"])
                    atlogger.debug(insert_query)
                    execute_query(insert_query)

        except Exception as e:
            atlogger.error(e)


    async def fetch_license_details(self, spring_headers):
        try:
            atlogger.debug("Going to fetch license details")

            graph_mgmt_url = 'https://graph.microsoft.com'
            token = acquire_authtoken(self.creds.tenantid, self.creds.
                                            applicationid, self.creds.appsecret, graph_mgmt_url)
            headers = form_auth_headers(token)
            licenses = LicenseDetails(token, headers)
            license_list = await licenses.fetch(spring_headers)
            for lics in license_list:
                license = LicenseDetails(token, headers)
                await license.parse(lics)
                self.licenses.append(license)

            return self.licenses
        except Exception as e:
            atlogger.error("Error in fetch_license_details = ".format((str(e))))
            return []
        
    async def fetch_user_alloc_err_details(self, spring_headers, sub_id):
        try:
            atlogger.debug("Going to fetch UserAllocationError details")
            Alloc_Err = UserAllocationError()
            Alloc_Err_list = await Alloc_Err.fetch(spring_headers, sub_id)
            for aler in Alloc_Err_list:
                Alloc_Error = UserAllocationError()
                await Alloc_Error.parse(aler)
                self.Alloc_Err.append(Alloc_Error)
            return self.Alloc_Err
        except Exception as e:
            atlogger.error("Error in fetch_user_alloc_err_details = ".format(str(e)))
            return []

    def to_xml(self,param):
        wvdxml = xml.Element('wvdDetails')
        if param==0:
            # Adding Tenent details
            tenxml = xml.Element('wvdTenants')
            if self.tenants != 0 or self.tenants != None:
                for ten in self.tenants:
                    txml = xml.Element('tenant')
                    txml.append(xml_tag('tenantName', ten.get('tenantName')))
                    txml.append(xml_tag('tenantGroupName', ten.get('tenantGroupName')))
                    txml.append(xml_tag('aadTenantId', ten.get('aadTenantId')))
                    txml.append(xml_tag('azureSubscriptionId',
                                    ten.get('azureSubscriptionId')))
                    tenxml.append(txml)
                    # Adding tenanat details under WVDDetails
                    wvdxml.append(tenxml)
            # Adding Hostpool details
            hstpoolxml = xml.Element('hostPoolInfo')
            for pool in self.hostpools:
                hstpoolxml.append(pool.to_xml())
                # Adding hostpooldetails under under WVD details
            wvdxml.append(hstpoolxml)

            # Adding SessionHost details
            sesshstxml = xml.Element('sessionHostInfo')
            for sess in self.sessionhosts:
                sesshstxml.append(sess.to_xml())
                # adding hostpooldetails under under wvd details
            wvdxml.append(sesshstxml)

            # Adding AppGroup Details
            appgrpxml = xml.Element('appGroupInfo')
            for grp in self.appgroups:
                appgrpxml.append(grp.to_xml(self.remotedesktops, self.remoteapps))
                # Adding appgroup detials under wvd details.
            wvdxml.append(appgrpxml)

            # Adding Workspace details
            wrkspxml = xml.Element('workspaceInfo')
            for wrks in self.workspaces:
                wrkspxml.append(wrks.to_xml())
                # adding workspace details under under wvd details
            wvdxml.append(wrkspxml)

            # Addinf ApplicationAndPackage Info
            appandpackageinfoxml = self.application_and_package_info.to_xml()
            appandpackageinfoxml_string = xml.tostring(appandpackageinfoxml, encoding="utf-8", pretty_print=True).decode("utf-8")
            atlogger.info("@VS application_and_package_info_xml_string :\n sub_id:{} \n {}".format(self.sub_id, appandpackageinfoxml_string))
            wvdxml.append(appandpackageinfoxml)

            # Adding License details
            licsxml = xml.Element('licenseInfo')
            print("licenses = ",self.licenses)
            for lics in self.licenses:
                print("for lics ",lics)
                licsxml.append(lics.to_xml())
            #wvdxml.append(licsxml)
            return wvdxml, licsxml
        elif param==1:

            # Adding user provisioned details
            usrxml = xml.Element('usersProvisioned')
            usrsxml = xml.Element('userInfo')
            for usr in self.userassignments:
                usr_xml = usr.user_to_xml()
                if usr_xml:
                    usrsxml.append(usr_xml)
                # adding workspace details under under wvd details
            if usrsxml:
                usrxml.append(usrsxml)
            
            group_xml = xml.Element('groupInfo')
            for grp in self.userassignments:
                grp_xml = grp.group_to_xml()
                if grp_xml:
                    group_xml.append(grp_xml)
            if group_xml:
                usrxml.append(group_xml)
            wvdxml.append(usrxml)

            uaexml = xml.Element('UserAllocationErrorInfo')
            for uae in self.userAllocError:
                uaexml.append(uae.to_xml())
            wvdxml.append(uaexml)
            return wvdxml, []

            return wvdxml, []


class TenantDetails:
    def __init__(self):
        self.tenant_params = ['tenantName', 'tenantGroupName', 'aadTenantId',
                              'azureSubscriptionId']
        # List to store tenant details
        self.tenant_info = []

    async def fetch(self, session, headers, wvd_base_url):
        tenant_url = "{0}{1}".format(wvd_base_url, '/Tenants')
        self.rdstenant = await get_details_async(session, headers,
                                                 tenant_url)
        if self.rdstenant == None:
           print('None self.rdstenant')
           return

        if len(self.rdstenant) == 0:
            print("Again trying")
            await asyncio.sleep(5)
            self.rdstenant = await get_details_async(session, headers,
                                                     tenant_url)

    async def parse(self):
        if self.rdstenant == None:
           return
        rdstenant_info = {a['tenantName']: a for a in self.rdstenant}.values()
        # print(rdstenant_info)
        for ten in rdstenant_info:
            tenantInfoDict = dict.fromkeys(self.tenant_params, "")
            tenantInfoDict['tenantName'] = ten.get('tenantName')
            tenantInfoDict['tenantGroupName'] = ten.get('tenantGroupName')
            tenantInfoDict['aadTenantId'] = ten.get('aadTenantId')
            tenantInfoDict['azureSubscriptionId'] = ten.get(
                'azureSubscriptionId')
            self.tenant_info.append(tenantInfoDict)
        return self.tenant_info


class WorkSpaceDetails:
    def __init__(self):
        self.workspaceName = ''
        self.description = ''
        self.friendlyName = ''
        # List of App grp references
        self.appGroupName = []
        self.resourceGroupName = ''

    async def fetch(self, headers, sub_id):
        async with aiohttp.ClientSession(headers=headers, trust_env=True) as session:
            wrks_url = 'https://management.azure.com/subscriptions/{0}/'\
                       'providers/Microsoft.DesktopVirtualization/workspaces'\
                       .format(sub_id)
            params = {"api-version": "2019-12-10-preview"}
            wrkspaceInfo = await get_details_async(session, headers, wrks_url,
                                                   params)
            return wrkspaceInfo['value']

    async def parse(self, wrks):
        self.workspaceName = wrks.get('name')
        self.description = wrks.get('properties', {}).get('description', "")
        self.friendlyName = wrks.get('properties', {}).get('friendlyName', "")
        rid = wrks.get('id')
        self.resourceGroupName = rid.split('/')[4]
        app_grp_ref = wrks.get('properties', {}).get(
            'applicationGroupReferences', [])
        self.appGroupName = [apg.split('/')[-1]
                             for apg in app_grp_ref]

    def to_xml(self):
        ws = xml.Element('workspace')
        ws.append(xml_tag('workspaceName', self.workspaceName))
        ws.append(xml_tag('description', self.description))
        ws.append(xml_tag('friendlyName', self.friendlyName))
        ws.append(xml_tag('resourceGroupName', self.resourceGroupName))
        apg_ref_xml = xml.Element('appGroupReferences')
        for apg in self.appGroupName:
            apg_ref_xml.append(xml_tag('appGroupName', apg))
        ws.append(apg_ref_xml)
        return ws

class UserAllocationError:
    def __init__(self):
        self.userName = ''
        self.hostName = ''
        self.link=''
        self.desiredValue=''
        self.error = ''
        self.timeStamp = ''
 
    async def fetch(self, headers, subid):
        try:
            async with aiohttp.ClientSession(headers=headers, trust_env=True) as allerror:
                current_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                past_date = datetime.now() - timedelta(days=30)
                past_date = past_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                url = "https://management.azure.com/subscriptions/{0}/providers/Microsoft.Insights/eventtypes/management/values?api-version=2015-04-01&$filter=eventTimestamp ge"+current_date+" and eventTimestamp le"+past_date
                allocError_url=url.format(subid)
                allocErrorInfo = await get_details_async(allerror, headers, allocError_url)
                return allocErrorInfo['value']
        except Exception as e:
            print(str(e))

    async def parse(self, allocation_err):
        if allocation_err["status"]["value"] == "Failed" and str(allocation_err["subStatus"]["value"]) == "AllocationFailed":
            self.userName = allocation_err.get["claims"]["name"]
            self.link = allocation_err.get["httpRequest"]["uri"]
            index_start = self.link.find("virtualMachines/") + len("virtualMachines/")
            index_end = self.link.find("/", index_start)
            if index_start != -1 and index_end != -1:
                self.desired_value=self.link[index_start:index_end]
                self.hostName=allocation_err.get(self.desired_value)
            self.error = allocation_err.get["properties"]["statusMessage"]
            self.timeStamp = allocation_err.get["eventTimestamp"]
 
    def to_xml(self):
        all = xml.Element('UserAllocationError')
        all.append(xml_tag('User_name', self.User_name))
        all.append(xml_tag('Host_name', self.Host_name))
        all.append(xml_tag('Error', self.Error))
        all.append(xml_tag('Timestamp', self.Timestamp))
        return all
   
class LicenseDetails:
    def __init__(self, token, headers):
        self.productName = ''
        self.total = ''
        self.assigned = ''
        self.available = ''
        self.expiringSoon = ''
        self.token = token
        self.headers = headers
        self.skuid = ''
    async def fetch(self, headers):
        try:
            async with aiohttp.ClientSession(headers=self.headers, trust_env=True) as license:
                license_url = 'https://graph.microsoft.com/v1.0/subscribedSkus'
                licenseInfo = await get_details_async(license, self.headers, license_url)
                atlogger.debug("License details %s", licenseInfo)
                return licenseInfo['value']
        except Exception as e:
            print(str(e))

    async def parse(self, lics):
        atlogger.debug("Type %s",type(lics))
        atlogger.debug("SkuPart %s",lics.get("skuPartNumber"))
        self.productName = lics.get('skuPartNumber')
        self.total = lics.get('prepaidUnits').get('enabled')
        atlogger.debug("prepaidUnits %s",self.total)
        self.assigned = lics.get('consumedUnits')
        atlogger.debug("consumedUnits %s",self.assigned)
        if self.total == 0:
            self.available = int(self.total)
        else:
            self.available = int(self.total) - int(self.assigned)
        atlogger.debug("available %s",str(self.available))
        self.expiringSoon = lics.get('prepaidUnits').get('warning')
        atlogger.debug("expiring  %s",self.expiringSoon)
        self.skuid = lics.get('skuId')
        atlogger.debug("skuid %s ",self.skuid)

    def to_xml(self):
        ls = xml.Element('license')
        if self.skuid in product_skuid_map.keys():
            ls.append(xml_tag('productName', product_skuid_map[self.skuid]))
        else:
            ls.append(xml_tag('productName', ' '.join(self.productName.split('_'))))
        ls.append(xml_tag('total', self.total))
        atlogger.debug("prepaidUnits %s",self.total)
        ls.append(xml_tag('assigned', self.assigned))
        ls.append(xml_tag('available', self.available))
        ls.append(xml_tag('expiringSoon', self.expiringSoon))
        atlogger.debug("License XML %s",ls)
        return ls


class VmUptime:
    def __init__(self,subid,tenantname, creds):
        self.subid = subid
        self.tenantname = tenantname
        self.tenantid = creds.tenantid
        self.clientid = creds.applicationid
        self.clientsecret = creds.appsecret

        self.log_headers = None
        self.maintenance = 0
        self.vmuptime = list()
        
        self.uptime_query = {'query': '''
            Heartbeat
                | where TimeGenerated > ago(30m)
                | where ComputerPrivateIPs !=""
                | summarize heartbeatPerMins = count() by bin_at(TimeGenerated, 1m, ago(7h)), Computer,     tostring(ComputerPrivateIPs)
                | extend availablePerHour = iff(heartbeatPerMins > 0, true, false)
                | summarize totalAvailableHours = countif(availablePerHour == true) by Computer, tostring(ComputerPrivateIPs)
                | join kind=inner (
                    WVDConnections
                    | extend Computer = SessionHostName
                    | distinct  Computer, _ResourceId
                ) on Computer
                | Union ( 
                    Event
                    | where EventLog == "System"
                    | where Source == "User32"
                    | where EventID == 1074
                    | where TimeGenerated > ago(30m)
                    | extend isRestart = iff(ParameterXml has "<Param>restart</Param>", true, false)
                    | summarize rebootCount = countif(isRestart== true) by Computer
                )
                | distinct Computer, tostring(ComputerPrivateIPs),totalAvailableHours,_ResourceId,rebootCount

        '''}
    
                  
    async def get_log_bearer(self,url):
            data = {
            "grant_type":"client_credentials",
            # "resource":"https://management.azure.com" if not isLog else "https://api.loganalytics.io",
            "client_id":self.clientid,
            "client_secret":self.clientsecret,
            "client_info":"1",
            "resource":"https://api.loganalytics.io"
            }
            token = None
            async with aiohttp.ClientSession(trust_env=True) as session:
                async with session.post(url,data=data) as response:
                    if response.status == 200:
                        token = await response.json()

            if token:
                self.log_headers={"Content-Type":"application/json","Authorization":f"Bearer {token['access_token']}"}


    async def postData(self,query):
            atlogger.debug(f"Fetching from log analytics")
            #data = requests.post(f"https://api.loganalytics.io/v1{self.subid}/query?scope=hierarchy&",json=query,headers=self.log_headers)
            data = None
            if self.log_headers:
                async with aiohttp.ClientSession(headers=self.log_headers,trust_env=True) as session:
                    async with session.post(f"https://api.loganalytics.io/v1/subscriptions/{self.subid}/query?scope=hierarchy&",json=query) as resp:
                        if resp.status == 200:
                            data = await resp.json() 
            atlogger.debug("@A vmuptime postdata: {}".format(data))
            return data


    def getTimeMins(self,time):
        hrs,mins = int(time.split(".")[0]),int(time.split(".")[1])
        mins = hrs*60+mins
        
        return str(mins)

    # totalHours = 24
    
    '''def insertIntoTable(self,poolname,vmip):
        endtime = datetime.now()
        starttime = endtime - timedelta(minutes=30)
        sel_query = f"select * from vmuptime where poolname='{poolname}' and serverip='{vmip}'"
        row = execute_query(sel_query)
        atlogger.debug(f"@CD row = {row}, select {sel_query}")
        if len(row)>0:
            atlogger.debug("Already Exists")
        else:
            query = f"insert into vmuptime values('{self.tenantname}','{poolname}','{vmip}','{self.uptime}','{self.reboot_count}','{self.maintenance}','{starttime}','{endtime}')"
            atlogger.debug(f"Inserted {query}")
            rowi = execute_query(query)'''
    def insertAllIntoTables(self):
        if self.vmuptime:
            bulk_insert("vmuptime","(%s,%s,%s,%s,%s,%s,%s,%s)",self.vmuptime)
        else:
            atlogger.error("Skipping db insert since self.vmuptime is empty")

        

    async def getDetails(self):
        try:           
            atlogger.debug("Started vmUpTime Report")
            #self.getConfig()
            await self.get_log_bearer(f"https://login.microsoftonline.com/{self.tenantid}/oauth2/token")
            res = await self.postData(self.uptime_query)
            print(f"Uptime {res}")
            if res:
                rows = res.get("tables",[])[0].get("rows",[])
                for row in rows:
                    hostname = row[0]
                    ip = json.loads(row[1])[0]
                    uptime = str(row[2])
                    poolname = row[3].split("hostpools/")[1]
                    reboot = row[4] if row[4] else 0
                    starttime = datetime.now()
                    endtime = starttime - timedelta(minutes=30)

                    self.vmuptime.append(
                        (   
                            self.tenantname,
                            poolname,
                            ip,
                            uptime,
                            reboot,
                            self.maintenance,
                            starttime,
                            endtime
                        )

                    )
                self.insertAllIntoTables()
    
            else:
                self.vmuptime = None
                atlogger.error("Skipping db insert since self.vmuptime is empty")
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            atlogger.error("%s:@CD GetDetails error vmuptime : %s %s",e, exc_type, str(exc_tb.tb_lineno))
        #reboot_count = 0
        

class HostPoolDetails:
    def __init__(self,tenantName,creds):
        self.tntname = tenantName
        self.creds = creds
        self.tenantName = ''
        self.tenantGroupName = ''
        self.hostPoolName = ''
        self.id = ''
        self.description = ''
        self.hostpoolType = ''
        self.pooltype = {True: "Personal", False: "Pooled"}
        self.customRdpProperty = ''
        self.maxSessionLimit = ''
        self.loadBalancerType = ''
        self.balancertype = {0: "BreadthFirst", 1: "DepthFirst",
                             2: "Personal"}
        self.validationEnv = ''
        self.ring = ''
        self.assignmentType = ''
        self.preferredAppGroupType = ''
        self.appGroupName = ''
        self.resourceGroupName = ''
        self.maintenanceMode = ''
        self.headers = ''
        self.subId = ''
        self.actualSize = ''
        self.vmRootName=''
        self.hostDomainName = ''
        self.defaultProtocol = ''
        self.vmimage=''
        self.vmIpMap = {}
        self.vmUptime = None  
    
    async def fetch_sessions(self, headers, subid, host, hostId):
        try:
            listResgrp = []
            async with aiohttp.ClientSession(headers=headers, trust_env=True) as session:

                session_url = 'https://management.azure.com/'+hostId+'/sessionHosts'+'?api-version=2023-09-05'
                sessionInfo = await get_details_async(session, headers, session_url)
                return len(sessionInfo['value'])
        except Exception as e:
            print(str(e))

    async def fetch_maintenance(self, headers, subid):
        try:
            listResgrp = []
            async with aiohttp.ClientSession(headers=headers, trust_env=True) as maintenance:

                maintenance_url = 'https://management.azure.com/subscriptions/{0}/'\
                                '/providers/Microsoft.Maintenance/maintenanceConfigurations?api-version=2023-04-01'\
                                .format(subid)
                maintenanceInfo = await get_details_async(maintenance, headers, maintenance_url)
                for i in range(len(maintenanceInfo["value"])):
                        listResgrp.append(maintenanceInfo["value"][i]["id"].split("/")[4]) 
                return listResgrp
        except Exception as e:
            print(str(e))

    async def fetch(self, fall_headers, spring_headers, ten_names,
                    sub_id):
        try:
            fall_base_url = 'https://rdbroker.wvd.microsoft.com/RdsManagement'\
                '/V1/TenantGroups/Default%20Tenant%20Group'
            print(ten_names)
            self.headers = spring_headers
            self.subId = sub_id
            hostpool_list_v1 = []
            if len(ten_names) > 0:
               for name in ten_names:
                   hostpoolInfo = await self.fetch_v1(fall_headers,
                                                      fall_base_url, name)
                   hostpool_list_v1.extend(hostpoolInfo)
            hostpool_list_v2 = await self.fetch_v2(spring_headers,
                                                   sub_id)
            maintenanceGrp = await self.fetch_maintenance(spring_headers, sub_id)
            atlogger.debug("@CD Before vmuptim report")
            vmuptime_report = VmUptime(sub_id,self.tntname,self.creds)
            await vmuptime_report.getDetails()
            return hostpool_list_v1, hostpool_list_v2, maintenanceGrp
        except Exception as e:
            print(str(e))
            return [],[]
    
    async def sql_executor(self, query):
        try:
            conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='postgres', database='anuntatech')
            curObj = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            atlogger.debug(f"Executing --> '{query}'")
            curObj.execute(query)
            conn.commit()
            curObj.close()
            conn.close()
            return True

        except Exception as e:
            atlogger.debug(f"Caught Execption '{e}' while executing {query}")
            return False


    async def generate_random_timestamp(self):
        current_time = datetime.now()
        random_milliseconds = random.randint(0, 999)
        random_timedelta = timedelta(milliseconds=random_milliseconds)
        random_timestamp = current_time + random_timedelta
        return random_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    async def fetch_vm(self, rpid, pool, headers):
        hostpoolUrl = pool.get('id', "")
        
        atlogger.info("Entering fetch_vm")
        pool_url = 'https://management.azure.com'+hostpoolUrl+'/sessionHosts'
        params = {"api-version": "2023-09-05"}

        sessionHostInfo = requests.get(pool_url,headers=headers, params=params)
        sessionHostInfo = sessionHostInfo.json()
        sessionHostInfo = sessionHostInfo['value']
        for session in sessionHostInfo:
            props = session.get('properties', {})
            vm_url = props.get('resourceId', "")
            query = f"let trendBinSize = 5m;let maxListSize = 1000;let rawDataCached = materialize(InsightsMetrics|where Origin == 'vm.azm.ms'| where _ResourceId =~ '{vm_url}'| extend Tags = todynamic(Tags), Val = iif(Name in ('WriteBytesPerSecond', 'ReadBytesPerSecond', 'BytesPerSecond'), Val/(1024.0 * 1024.0), iif(Name in ('WriteLatencyMs', 'ReadLatencyMs', 'TransferLatencyMs'), Val/1000.0, Val))| extend InstanceName = tostring(Tags['vm.azm.ms/mountId']),DiskSizeMB = todecimal(Tags['vm.azm.ms/diskSizeMB'])| project TimeGenerated,Name,InstanceName,DiskSizeMB,Val = case(Val < 0, real(0),Val));let TotalDiskMetrics = rawDataCached| where Name != 'FreeSpaceMB'| summarize Val = sum(Val) by bin(TimeGenerated, 1m), Name| summarize P95 = percentile(Val, 95), Count=count() by Name, InstanceName = '_Total';let SeperateUsedDiskSpace = rawDataCached| where Name == 'FreeSpaceMB'| summarize ArgMaxVal = arg_max(TimeGenerated, *) by InstanceName| project InstanceName, DiskSizeMB, FreeMB = Val;let TotalUsedDiskSpace = SeperateUsedDiskSpace| summarize DiskSizeMB = sum(DiskSizeMB), FreeMB = sum(FreeMB) by InstanceName = '_Total'| project InstanceName, DiskSizeMB, FreeMB;let AllDiskUsedSpace =  TotalUsedDiskSpace| project InstanceName, DiskSizeGB = round(DiskSizeMB/1024.0, 2), UsedGB = round((DiskSizeMB - FreeMB)/1024.0, 2);rawDataCached| union TotalDiskMetrics| join AllDiskUsedSpace on InstanceName| project InstanceName, DiskSizeGB, UsedGB"
            body={"query":f"{query}"}

            vm_post_url= 'https://management.azure.com'+vm_url+"?api-version=2023-09-01"
            atlogger.debug(f"@CD vm_url {vm_post_url}")
            vmInsights_url = 'https://management.azure.com'+vm_url+'/providers/microsoft.insights/logs?api-version=2018-08-01-preview&scope=hierarchy'
            vm_resp = requests.get(vm_post_url, headers=headers)
            if vm_resp.status_code != 200:
                atlogger.error(f"vmSession error = {vm_resp.json()}")
                continue
            else:
                vm_resp = vm_resp.json()
            try:
                total, used = 0, 0
                vmInsightsInfo = requests.post(vmInsights_url, json=body,headers=headers).json()
                if not "error" in vmInsightsInfo:
                    if "tables" in vmInsightsInfo:
                        if  len(vmInsightsInfo['tables'][0]['rows'])!=0:
                            total = vmInsightsInfo['tables'][0]['rows'][0][1]
                            used = vmInsightsInfo['tables'][0]['rows'][0][2]
                            
                        timeCreated=get_current_time()
                        dt_object = datetime.strptime(timeCreated, "%Y-%m-%dT%H:%M:%SZ")
                        startTime = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)

                        startTime = startTime.strftime("%Y-%m-%d %H:%M:%S.%f")
                        query = f"insert into diskcapacityusage(resourcepoolid, poolname, servername, total, used, start_time, end_time) values('{rpid}','{self.hostPoolName}','{vm_resp['name']}',{total},{used},'{startTime}','{timeCreated}')"
                        await self.sql_executor(query)

                #Fetching upTime report data
                atlogger.debug("@CD before vmUptime")
                self.vmUptime = VmUptime(vm_url,self.tntname,headers,vm_resp['name'],self.creds)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                atlogger.error("@HJ %s:@CD Error : %s %s",e, exc_type, str(exc_tb.tb_lineno))
            
            
            ###########################
            privateIpaddress = None
            try:
                if "properties" in vm_resp.keys():
                    if "storageProfile" in vm_resp['properties']:
                        if "imageReference" in vm_resp['properties']['storageProfile']:
                            if 'id' in vm_resp['properties']['storageProfile']['imageReference']: 
                                vm_image = vm_resp['properties']['storageProfile']['imageReference']['id'].split('/')[-3]
                                self.vmimage = vm_image
                            else:
                                self.vmimage = None
            except Exception as e:
                atlogger.debug("@VS properties exception {}".format(e))
                exc_type, exc_obj, exc_tb = sys.exc_info()
                atlogger.error("@HJ %s: Error : %s %s",e, exc_type, str(exc_tb.tb_lineno))

            try:
                if "properties" in vm_resp.keys():
                    if "networkProfile" in vm_resp['properties']:
                        if "networkInterfaces" in vm_resp['properties']['networkProfile']:
                            vm_net = vm_resp['properties']['networkProfile']['networkInterfaces'][0]['id']
                            atlogger.debug(f"@HJ vmnet sliced sucessfully --> {vm_net}")
                            netUrl =  'https://management.azure.com'+vm_net+"?api-version=2023-09-01"

                            netDetails = requests.get(netUrl, headers=headers).json()
                            atlogger.debug(f"@HJ netDetails fetched successfully --> {netDetails}")
                            if "properties" in netDetails.keys():
                                if "ipConfigurations" in netDetails['properties']:
                                    if "properties" in netDetails['properties']['ipConfigurations'][0]:
                                        self.vmIpMap[vm_resp['name']] = netDetails['properties']['ipConfigurations'][0]['properties']['privateIPAddress']
                                        #self.privateIpaddress.append(netDetails['properties']['ipConfigurations'][0]['properties']['privateIPAddress'])

                                        if self.vmIpMap.keys():
                                            timecreated = get_current_time()
                                            for vmName, vmIp in self.vmIpMap.items():
                                                query = f"update diskcapacityusage set serverip='{vmIp}' where poolname='{self.hostPoolName}' and servername='{vmName}'"
                                                await self.sql_executor(query)
                                                if self.vmUptime != None:
                                                    atlogger.debug(f"@CD before getDetails, vmName={vmName} vmIp={vmIp} ")
                                                    self.vmUptime.getDetails(self.hostPoolName,vmIp)

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
	        
                atlogger.error("@HJ %s: Error : %s %s",e, exc_type, str(exc_tb.tb_lineno))
            
           

            ###########################
            try: 
                headers["Content-Type"] = "application/json"
                resp=requests.post(vmInsights_url, json=body,headers=headers)
                if resp.status_code!=200:
                    atlogger.debug("@HJ Failed with status code --> %s %s",resp.status_code, resp.text)
                else:
                    atlogger.debug('@A disk util: %s', resp.json())
            except:
                atlogger.debug("@VS properties exception Except: %s",e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                atlogger.error("@HJ %s: Error : %s %s",e, exc_type, str(exc_tb.tb_lineno))

    async def parse(self, hostpool_list_v1, hostpool_list_v2, maintenanceGrp, headers):
        fall_base_url = 'https://rdbroker.wvd.microsoft.com/RdsManagement'\
            '/V1/TenantGroups/Default%20Tenant%20Group'
        hostpools = []
        for pool in hostpool_list_v1:
            hstpool = HostPoolDetails(self.tntname,self.creds)
            await hstpool.parse_v1(pool, fall_base_url)
            hostpools.append(hstpool)
        for pool in hostpool_list_v2:
            systemData = pool.get('systemData', {})
            if systemData['createdByType'] == 'User':
                self.hostDomainName = systemData['createdBy'].split("@")[1]
        for pool in hostpool_list_v2:
            hstpool = HostPoolDetails(self.tntname,self.creds)
            self.actualSize = await self.fetch_sessions(self.headers, self.subId, pool['name'], pool['id'])
            await hstpool.parse_v2(pool, maintenanceGrp, self.hostDomainName, self.actualSize, headers)
            hostpools.append(hstpool)
        return hostpools

    async def fetch_v1(self, headers, fall_base_url, tenantname):
        async with aiohttp.ClientSession(headers=headers, trust_env=True) as session:
            rdsHostPoolUrl = "{0}{1}{2}{3}".format(fall_base_url, '/Tenants/',
                                                   tenantname, '/HostPools')
            hostPoolInfo = await get_details_async(session, headers, rdsHostPoolUrl)
            print(hostPoolInfo)
            return hostPoolInfo

    async def fetch_v2(self, headers, subid):
        async with aiohttp.ClientSession(headers=headers, trust_env=True) as session:
            pool_url = 'https://management.azure.com/subscriptions/{0}/'\
                       'providers/Microsoft.DesktopVirtualization/hostPools'\
                       .format(subid)
            params = {"api-version": "2019-12-10-preview"}
            hostPoolInfo = await get_details_async(session, headers, pool_url,
                                                   params)
            print(hostPoolInfo)
            return hostPoolInfo['value']

    async def parse_v1(self, pool, fall_base_url):
        self.tenantName = pool.get('tenantName')
        self.tenantGroupName = pool.get('tenantGroupName', "")
        self.hostPoolName = pool.get('hostPoolName', "")
        self.id = "{0}{1}{2}{3}{4}".format(fall_base_url, '/Tenants/',
                                           self.tenantName, '/HostPools/', self.hostPoolName)
        self.description = pool.get('description')
        hptype = pool.get('persistent')
        self.hostpoolType = self.pooltype[hptype]
        self.customRdpProperty = pool.get('customRdpProperty')
        self.maxSessionLimit = str(pool.get('maxSessionLimit'))
        lb_type = pool.get('loadBalancerType')
        self.loadBalancerType = self.balancertype[lb_type]
        self.validationEnv = str(pool.get('validationEnv'))
        self.ring = str(pool.get('ring'))
        self.assignmentType = str(pool.get('assignmentType'))

    async def parse_v2(self, pool, maintenanceGrp, domain, size, headers):
        self.hostPoolName = pool.get('name', "")
        self.id = pool.get('id', "")
        self.actualSize = size
        self.hostDomainName = domain
        props = pool.get('properties', {})
        systemData = pool.get('systemData', {})
        self.defaultProtocol
        self.defaultProtocol= 'RDP'
        self.description = props.get('description', "")
        self.vmRootName = self.hostPoolName
        self.hostpoolType = props.get('hostPoolType', "")
        if self.hostpoolType == 'Personal':
            self.sessionBased = 'No'
        else:
            self.sessionBased = 'Yes'
        self.customRdpProperty = props.get('customRdpProperty', "")
        self.maxSessionLimit = str(props.get('maxSessionLimit', ""))
        self.loadBalancerType = props.get('loadBalancerType', "")
        self.validationEnv = str(props.get('validationEnvironment', ""))
        self.ring = str(props.get('ring', ""))
        self.assignmentType = str(props.get
                                  ('personalDesktopAssignmentType', ""))
        self.preferredAppGroupType = props.get('preferredAppGroupType', "")
        app_grp_ref = props.get('applicationGroupReferences', [])
        self.appGroupName = ""
        if app_grp_ref != []:
           self.appGroupName = [apg.split('/')[-1]
                                for apg in app_grp_ref]
           self.appGroupName = self.appGroupName[0]
        self.resourceGroupName = self.id.split('/')[4]
        if self.id.split("/")[4] in maintenanceGrp:
            self.maintenanceMode = "On"
        else:
            self.maintenanceMode = "Off"
        
        timeCreated = get_current_time()
        #atlogger.debug('@A vmperpool timecreated is %s %s', endTime, type(endTime))
        dt_object = datetime.strptime(timeCreated, "%Y-%m-%dT%H:%M:%SZ")
        startTime = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
        startTime = startTime.strftime("%Y-%m-%dT%H:%M:%SZ")
        """await self.fetch_vm(self.tntname, pool, headers)
        for vmName, vmIp in self.vmIpMap.items():
            endTime = await self.generate_random_timestamp()
            query = f"insert into vmperpool(resourcepoolid, poolname, start_time, end_time, servername, serverip) values('{self.tntname}','{self.hostPoolName}', '{startTime}', '{endTime}', '{vmName}', '{vmIp}')"
            atlogger.debug(f'@A Entering func with query {query}')
            check_flag = await self.sql_executor(query)
            if check_flag:
                atlogger.debug("Insert query successfull")
            else:
                atlogger.error("Error in insert query")"""

        
        

    def to_xml(self):
        hp = xml.Element('hostPool')
        hp.append(xml_tag('maintenanceMode', self.maintenanceMode))
        hp.append(xml_tag('tenantName', self.tenantName))
        hp.append(xml_tag('tenantGroupName', self.tenantGroupName))
        hp.append(xml_tag('hostPoolName', self.hostPoolName))
        hp.append(xml_tag('actualSize', self.actualSize))
        hp.append(xml_tag('requestedSize', self.actualSize))
        hp.append(xml_tag('vmRootName', self.vmRootName))
        hp.append(xml_tag('defaultProtocol', self.defaultProtocol))
        hp.append(xml_tag('domainName', self.hostDomainName))
        hp.append(xml_tag('sessionBased', self.sessionBased))
        hp.append(xml_tag('description', self.description))
        hp.append(xml_tag('hostpoolType', self.hostpoolType))
        hp.append(xml_tag('customRdpProperty', self.customRdpProperty))
        hp.append(xml_tag('maxSessionLimit', self.maxSessionLimit))
        hp.append(xml_tag('loadBalancerType', self.loadBalancerType))
        hp.append(xml_tag('validationEnv', self.validationEnv))
        hp.append(xml_tag('ring', self.ring))
        hp.append(xml_tag('assignmentType', self.assignmentType))
        hp.append(xml_tag('preferredAppGroupType',
                          self.preferredAppGroupType))
        hp.append(xml_tag('appGroupName', self.appGroupName))
        hp.append(xml_tag('resourceGroupName', self.resourceGroupName))
        hp.append(xml_tag('mode','Enabled'))
        hp.append(xml_tag('imageName', self.vmimage))
        return hp


class SessionHostDetails:
    def __init__(self, tenant_name):
        self.tenantName = tenant_name
        self.tenantGroupName = ''
        self.hostPoolName = ''
        self.sessionHostName = ''
        self.allowNewSession = ''
        self.sessions = ''
        self.lastHeartBeat = ''
        self.agentVersion = ''
        self.assignedUser = ''
        self.osVersion = ''
        self.sxSStackVersion = ''
        self.status = ''
        self.vmstatus = {0: "Available", 1: "Disconnected", 2: "Shutdown",
                         3: "Unavailable", 4: "UpgradeFailed", 5: "Upgrading"}
        self.statusTimestamp = ''
        self.updateState = ''
        self.update_state = {0: "Failed", 1: "Initial", 2: "Initial",
                             3: "Started", 4: "Succeeded"}
        self.lastUpdateTime = ''
        self.updateErrorMessage = ''
        self.vheaders = ''
        self.ipAddress = ''
        self.assignedState = 'UnAssigned'
        self.virtualMachineId = ''
        self.vmResourceId = ''
        self.vmName = ''
        self.hostPoolResourceId = ''

    async def fetch(self, fall_headers, spring_headers, hostpools):
        try:
            url_v1 = ["{0}{1}".format(hst.id, '/SessionHosts')
                      for hst in hostpools if 'rdbroker' in hst.id]
            url_v2 = ["{0}{1}{2}".format('https://management.azure.com', hst.id,
                                         '/sessionHosts') for hst in hostpools
                      if 'rdbroker' not in hst.id]
            sesshost_list_v1 = await self.fetch_v1(url_v1, fall_headers)
            sesshost_list_v2 = await self.fetch_v2(url_v2, spring_headers)

            return sesshost_list_v1, sesshost_list_v2
        except Exception as e:
            print(str(e))
            return [],[]

    async def parse(self, sesshost_list_v1, sesshost_list_v2, headers):
        sessionhosts = []
        for host in sesshost_list_v1:
            sesshst = SessionHostDetails(self.tenantName)
            await sesshst.parse_v1(host)
            sessionhosts.append(sesshst)
        for host in sesshost_list_v2:
            atlogger.debug("HostInfo:%s %s", host, type(host))
            host1 = host.get('value')
            for hst in host1:
                sesshst = SessionHostDetails(self.tenantName)
                await sesshst.parse_v2(hst, headers)
                sessionhosts.append(sesshst)            
        return sessionhosts

    async def fetch_v1(self, urls, headers):
        try:
            sessionHostInfo = await get_async(urls, headers)
            print(sessionHostInfo)
            sess_hst_info = [hst for sess in sessionHostInfo for hst in sess]
            return sess_hst_info
        except Exception as e:
            return []

    async def fetch_v2(self, urls, headers):
        try:
            self.vheaders = headers
            params = {"api-version": "2019-12-10-preview"}
            sessionHostInfo = await get_async(urls, headers, params)
            return sessionHostInfo#[0]['value']
        except Exception as e:
            return []

    async def parse_v1(self, host):
        self.tenantName = host.get('tenantName')
        self.tenantGroupName = host.get('tenantGroupName')
        self.hostPoolName = host.get('hostPoolName')
        self.sessionHostName = host.get('sessionHostName')
        self.allowNewSession = host.get('allowNewSession')
        self.sessions = str(host.get('sessions'))
        self.lastHeartBeat = host.get('lastHeartBeat')
        self.agentVersion = host.get('agentVersion')
        self.assignedUser = host.get('assignedUser')
        self.osVersion = host.get('osVersion')
        self.sxSStackVersion = host.get('sxSStackVersion')
        vm_sts = host.get('status')
        self.status = self.vmstatus[vm_sts]
        upd_state = host.get('updateState')
        self.updateState = self.update_state[upd_state]
        self.lastUpdateTime = host.get('lastUpdateTime')
        self.updateErrorMessage = host.get('updateErrorMessage')

    async def parse_v2(self, host, headers):
        name = host.get('name')
        sessionHostUrl=host.get('id')
        self.sessionHostId = host.get('id')
        name_split = name.split('/')
        self.hostPoolName = name_split[0]
        self.hostPoolResourceId = "/".join(self.sessionHostId.split("/")[:-2])
        self.sessionHostName = name_split[1]
        props = host.get('properties', {})
        self.allowNewSession = props.get('allowNewSession', "")
        self.sessions = str(props.get('sessions', ""))
        self.lastHeartBeat = props.get('lastHeartBeat', "")
        self.agentVersion = props.get('agentVersion', "")
        self.assignedUser = props.get('assignedUser', "")
        if self.assignedUser:
            self.assignedState = 'Assigned'
        self.osVersion = props.get('osVersion', "")
        self.sxSStackVersion = props.get('sxSStackVersion', "")
        self.status = props.get('status', "")
        self.updateState = props.get('updateState', "")
        self.lastUpdateTime = props.get('lastUpdateTime', "")
        self.updateErrorMessage = props.get('updateErrorMessage', "")
        self.statusTimestamp = props.get('statusTimestamp', "")
        self.virtualMachineId = props.get('virtualMachineId', "")
        self.vmResourceId = props.get('resourceId', "")

        vm_url = []
        vm_net_url = []
        mng_url = "https://management.azure.com"
        params = {"api-version": "2015-05-01"}
        vm_url.append( mng_url + str(props.get('resourceId', ""))+'?api-version=2015-05-01-preview')
        vm_url_1 = mng_url + str(props.get('resourceId', ""))
        grp_name_url = vm_url_1.split('/providers')[0]

        token=headers['Authorization']
        headers = {"Authorization": token}
        vmInformation=requests.get(vm_url[0],headers=headers).json()
        #vmInformation = await get_async(vm_url,headers, params)
        atlogger.debug("vmInformation @@ %s",vmInformation)
        #storageName,avaName = await self.fetch_SA_AV(grp_name_url,headers) 
        #atlogger.debug("@CD StorageName: %s", storageName)
        if 'name' in vmInformation:
            self.vmName = vmInformation.get('name', '')
            vm_props = vmInformation.get('properties', {})
            vm_net_prof = vm_props.get('networkProfile', {})
            vm_net_int = vm_net_prof.get('networkInterfaces', {})[0]
            vm_net_url.append(mng_url + str(vm_net_int.get('id', ""))+'?api-version=2014-12-01-preview')
            netInformation=requests.get(vm_net_url[0],headers=headers).json()
            #netInformation = await get_async(vm_net_url,headers, {"api-version": "2023-06-01"})
            #netInformation = netInformation[0]
            net_props = netInformation.get('properties', {})
            net_ip = net_props.get('ipConfigurations', {})[0]
            net_ip_prop = net_ip.get('properties', {})
            self.ipAddress = net_ip_prop.get('privateIPAddress', '')

            timeCreated = get_current_time()
            #atlogger.debug('@A vmperpool timecreated is %s %s', endTime, type(endTime))
            dt_object = datetime.strptime(timeCreated, "%Y-%m-%dT%H:%M:%SZ")
            startTime = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
            startTime = startTime.strftime("%Y-%m-%dT%H:%M:%SZ")

            try:
                query = f"insert into vmperpool(resourcepoolid, poolname, start_time, end_time, servername, serverip) values('{self.tenantName}','{self.hostPoolName}', '{startTime}', '{timeCreated}', '{self.vmName}', '{self.ipAddress}')"
                atlogger.debug(f'@A Entering func with query {query}')
                execute_query(query)
            except Exception as e:
                atlogger.error("Failed to fetch vmperpool: %s", e)
        #except:
        #    self.assignedState='UnAssigned'
        #    atlogger.debug("No VMs Available")

    def to_xml(self):
        sh = xml.Element('sessionHost')
        sh.append(xml_tag('tenantName', self.tenantName))
        sh.append(xml_tag('tenantGroupName', self.tenantGroupName))
        sh.append(xml_tag('hostPoolName', self.hostPoolName))
        sh.append(xml_tag('sessionHostName', self.sessionHostName))
        sh.append(xml_tag('allowNewSession', self.allowNewSession))
        sh.append(xml_tag('sessions', self.sessions))
        sh.append(xml_tag('lastHeartBeat', self.lastHeartBeat))
        sh.append(xml_tag('agentVersion', self.agentVersion))
        sh.append(xml_tag('assignedUser', self.assignedUser))
        sh.append(xml_tag('osVersion', self.osVersion))
        sh.append(xml_tag('sxSStackVersion', self.sxSStackVersion))
        sh.append(xml_tag('status', self.status))
        sh.append(xml_tag('updateState', self.updateState))
        sh.append(xml_tag('lastUpdateTime', self.lastUpdateTime))
        sh.append(xml_tag('updateErrorMessage', self.updateErrorMessage))
        sh.append(xml_tag('statusTimestamp', self.lastUpdateTime))
        sh.append(xml_tag('assignedState',self.assignedState))
        sh.append(xml_tag('ipAddress',self.ipAddress))
        return sh


class AppGroupDetails:
    def __init__(self):
        self.tenantName = ''
        self.tenantGroupName = ''
        self.hostPoolName = ''
        self.appGroupName = ''
        self.description = ''
        self.friendlyName = ''
        self.applicationGroupType = ''
        self.resource_type = {0: "RemoteApp", 1: "Desktop"}
        self.workspaceName = ''
        self.id = ''
        self.hostpool_armpath = ""
        self.workspace_armpath = ""

    async def fetch(self, fall_headers, spring_headers, hostpools, sub_id):
        try:
            custom_sub_id = sub_id.split('/')[0]
            url_v1 = ["{0}{1}".format(hst.id, '/AppGroups')
                      for hst in hostpools if 'rdbroker' in hst.id]
            appgrp_list_v1 = await self.fetch_v1(url_v1, fall_headers)
            appgrp_list_v2 = await self.fetch_v2(spring_headers, custom_sub_id)
            # sesshost_list_v2 = await self.fetch_v2(url_v2, spring_headers)
            return appgrp_list_v1, appgrp_list_v2
        except Exception as e:
            print(str(e))
            return [],[]

    async def parse(self, appgrp_list_v1, appgrp_list_v2):
        fall_base_url = 'rdbroker.wvd.microsoft.com/RdsManagement'\
            '/V1/TenantGroups/Default Tenant Group'
        appgroups = []
        for app in appgrp_list_v1:
            appgrp = AppGroupDetails()
            await appgrp.parse_v1(app, fall_base_url)
            appgroups.append(appgrp)
        for app in appgrp_list_v2:
            appgrp = AppGroupDetails()
            await appgrp.parse_v2(app)
            appgroups.append(appgrp)
        return appgroups

    async def fetch_v1(self, urls, headers):
        appGroupInfo = await get_async(urls, headers)
        app_grp_info = [grp for apg in appGroupInfo for grp in apg]
        return app_grp_info

    async def fetch_v2(self, headers, sub_id):
        async with aiohttp.ClientSession(headers=headers, trust_env=True) as session:
            appgrp_url = 'https://management.azure.com/subscriptions/{0}/'\
                'providers/Microsoft.DesktopVirtualization/applicationGroups'\
                .format(sub_id)
            params = {"api-version": "2019-12-10-preview"}
            appGroupInfo = await get_details_async(session, headers,
                                                   appgrp_url, params)
            return appGroupInfo['value']

    async def parse_v1(self, app, fall_base_url):
        self.tenantName = app.get('tenantName')
        self.tenantGroupName = app.get('tenantGroupName')
        self.hostPoolName = app.get('hostPoolName')
        self.appGroupName = app.get('appGroupName')
        self.description = app.get('description')
        self.friendlyName = app.get('friendlyName')
        res_type = app.get('resourceType')
        self.applicationGroupType = self.resource_type[res_type]
        # Store appgrp url to get desktop/remoteapp/user prov details
        url = "{0}{1}{2}{3}{4}{5}{6}".format(fall_base_url, '/Tenants/',
                                             self.tenantName, '/HostPools/', self.hostPoolName,
                                             '/AppGroups/', self.appGroupName)
        url = urllib.parse.quote(url)
        self.id = "{0}{1}".format('https://', url)

    async def parse_v2(self, app):
        props = app.get('properties', {})
        hpool_path = props.get('hostPoolArmPath')
        self.hostpool_armpath = hpool_path
        hpoolname = ""
        if hpool_path != None:
           hpoolname = (hpool_path.split('/'))[-1]
        self.hostPoolName = hpoolname#[-1]
        self.appGroupName = app.get('name')
        gppgrp_id = app.get('id')
        self.id = "{0}{1}".format("https://management.azure.com", gppgrp_id)
        self.description = props.get('description')
        self.friendlyName = props.get('friendlyName')
        self.applicationGroupType = props.get('applicationGroupType', "")
        wrkspace_path = props.get('workspaceArmPath')
        self.workspace_armpath = wrkspace_path
        wrkspace_name = ""
        if wrkspace_path != None:
           wrkspace_name = (wrkspace_path.split('/'))[-1]
        atlogger.debug("hostpool data workspace path is %s", wrkspace_path)
        #wrkspace_name = wrkspace_path.split('/')
        self.workspaceName = wrkspace_name#[-1]

    def to_xml(self, remotedesktops, remoteapps):
        ag = xml.Element('appGroup')
        ag.append(xml_tag('tenantName', self.tenantName))
        ag.append(xml_tag('tenantGroupName', self.tenantGroupName))
        ag.append(xml_tag('appGroupName', self.appGroupName))
        ag.append(xml_tag('hostPoolName', self.hostPoolName))
        ag.append(xml_tag('description', self.description))
        ag.append(xml_tag('friendlyName', self.friendlyName))
        ag.append(xml_tag('applicationGroupType',
                          self.applicationGroupType))
        ag.append(xml_tag('workspaceName', self.workspaceName))
        remdesxml = xml.Element('remoteDesktopInfo')
        if self.applicationGroupType == 'Desktop':
            remdesxml.append(self.get_remdes_info(self.appGroupName,
                                                  self.hostPoolName, remotedesktops))
        ag.append(remdesxml)
        remappxml = xml.Element('remoteAppInfo')
        if self.applicationGroupType == 'RemoteApp':
            remappxml.append(self.get_remapp_info(self.appGroupName,
                                                  self.hostPoolName, remoteapps))
        ag.append(remappxml)
        return ag

    def get_remdes_info(self, appGroupName, hostPoolName, remotedesktops):
        desxml = xml.Element('Desktop')
        for desktop in remotedesktops:
            if appGroupName == desktop.appGroupName and\
                    hostPoolName == desktop.hostPoolName:
                desxml.append(xml_tag('tenantName', desktop.tenantName))
                desxml.append(xml_tag('tenantGroupName',
                                      desktop.tenantGroupName))
                desxml.append(xml_tag('appGroupName', desktop.appGroupName))
                desxml.append(xml_tag('hostPoolName', desktop.hostPoolName))
                desxml.append(xml_tag('remoteDesktopName',
                                      desktop.remoteDesktopName))
                desxml.append(xml_tag('showInWebFeed',
                                      desktop.showInWebFeed))
        return desxml

    def get_remapp_info(self, appGroupName, hostPoolName, remoteapps):
        appxml = xml.Element('remoteApp')
        for app in remoteapps:
            if appGroupName == app.appGroupName and\
                    hostPoolName == app.hostPoolName:
                appxml.append(xml_tag('tenantName', app.tenantName))
                appxml.append(xml_tag('tenantGroupName', app.tenantGroupName))
                appxml.append(xml_tag('appGroupName', app.appGroupName))
                appxml.append(xml_tag('hostPoolName', app.hostPoolName))
                appxml.append(xml_tag('remoteAppName',
                                      app.remoteAppName))
                appxml.append(xml_tag('showInWebFeed',
                                      app.showInWebFeed))
        return appxml


class RemoteDesktopDetails:
    def __init__(self):
        self.tenantName = ''
        self.tenantGroupName = ''
        self.hostPoolName = ''
        self.remoteDesktopName = ''
        self.showInWebFeed = ''

    async def fetch(self, fall_headers, spring_headers, appgroups):
        try:
            url_v1 = ["{0}{1}".format(grp.id, '/Desktop')
                      for grp in appgroups if 'rdbroker' in grp.id and
                      grp.applicationGroupType == 'Desktop']
            url_v2 = ["{0}{1}".format(grp.id, '/desktops')
                      for grp in appgroups if 'rdbroker' not in grp.id and
                      grp.applicationGroupType == 'Desktop']
            print(url_v1)
            print(url_v2)
            redes_list_v1 = await self.fetch_v1(url_v1, fall_headers)
            redes_list_v2 = await self.fetch_v2(url_v2, spring_headers)

            return redes_list_v1, redes_list_v2
        except Exception as e:
            print(str(e))
            return [],[]

    async def parse(self, redes_list_v1, redes_list_v2, appgroups):
        remotedesktops = []
        for des in redes_list_v1:
            remdes = RemoteDesktopDetails()
            await remdes.parse_v1(des)
            remotedesktops.append(remdes)
        for des in redes_list_v2:
            remdes = RemoteDesktopDetails()
            rdes = des.get('value')
            if rdes == None:
               continue
            for rd in rdes:
                remdes = RemoteDesktopDetails()
                await remdes.parse_v2(rd, appgroups)
                remotedesktops.append(remdes)
        return remotedesktops

    async def fetch_v1(self, urls, headers):
        try:
            remoteDesInfo = await get_async(urls, headers)
            print(remoteDesInfo)
            return remoteDesInfo
        except Exception as e:
            print(str(e))
            return []

    async def fetch_v2(self, urls, headers):
        try:
            params = {"api-version": "2019-12-10-preview"}
            remoteDesInfo = await get_async(urls, headers, params)
            print(remoteDesInfo)
            #return remoteDesInfo[0]['value'] if len(remoteDesInfo) > 0 \
            #    else remoteDesInfo
            return remoteDesInfo
        except Exception as e:
            print(str(e))
            return []

    async def parse_v1(self, des):
        self.tenantName = des.get('tenantName')
        self.tenantGroupName = des.get('tenantGroupName')
        self.hostPoolName = des.get('hostPoolName')
        self.appGroupName = des.get('appGroupName')
        self.remoteDesktopName = des.get('remoteDesktopName')
        self.showInWebFeed = des.get('showInWebFeed')

    async def parse_v2(self, des, appgroups):
        name = des.get('name')
        name_split = name.split('/')
        self.hostPoolName = ''
        self.appGroupName = name_split[0]
        self.remoteDesktopName = name_split[1]
        self.showInWebFeed = None
        for grp in appgroups:
            if grp.appGroupName == self.appGroupName:
               self.hostPoolName = grp.hostPoolName
               #atlogger.debug("##Hpool: %s, appgrp: %s, rdname: %s", self.hostPoolName, self.appGroupName, self.remoteDesktopName)
               return

class RemoteAppDetails:
    def __init__(self):
        self.tenantName = ''
        self.tenantGroupName = ''
        self.hostPoolName = ''
        self.remoteAppName = ''
        self.showInWebFeed = ''

    async def fetch(self, fall_headers, spring_headers, appgroups):
        try:
            url_v1 = ["{0}{1}".format(grp.id, '/RemoteApps')
                      for grp in appgroups if 'rdbroker' in grp.id and
                      grp.applicationGroupType == 'RemoteApp']
            url_v2 = ["{0}{1}".format(grp.id, '/applications')
                      for grp in appgroups if 'rdbroker' not in grp.id and
                      grp.applicationGroupType == 'RemoteApp']
            remapp_list_v1 = await self.fetch_v1(url_v1, fall_headers)
            remapp_list_v2 = await self.fetch_v2(url_v2, spring_headers)

            return remapp_list_v1, remapp_list_v2
        except Exception as e:
            print(str(e))
            return [],[]

    async def parse(self, remapp_list_v1, remapp_list_v2):
        remoteapps = []
        for app in remapp_list_v1:
            remapp = RemoteAppDetails()
            await remapp.parse_v1(app)
            remoteapps.append(remapp)
        '''
        for app in remapp_list_v2:
            remapp = RemoteAppDetails() 
            await remapp.parse_v2(app)
            remoteapps.append(remapp)
        '''
        return remoteapps

    async def fetch_v1(self, urls, headers):
        try:
            remoteAppInfo = await get_async(urls, headers)
            print(remoteAppInfo)
            return [app for re_app in remoteAppInfo for app in re_app]
        except Exception as e:
            print(str(e))
            return []

    async def fetch_v2(self, urls, headers):
        try:
            params = {"api-version": "2019-12-10-preview"}
            remoteAppInfo = await get_async(urls, headers, params)
            print("Spring")
            print(remoteAppInfo)
            # return remoteDesInfo[0]['value']
        except Exception as e:
            print(str(e))
            return []

    async def parse_v1(self, app):
        self.tenantName = app.get('tenantName')
        self.tenantGroupName = app.get('tenantGroupName')
        self.hostPoolName = app.get('hostPoolName')
        self.appGroupName = app.get('appGroupName')
        self.remoteAppName = app.get('remoteAppName')
        self.showInWebFeed = app.get('showInWebFeed')

    async def parse_v2(self):
        pass


class UserProvisionedDetails:
    def __init__(self):
        self.userPrincipalName = ''
        self.tenantName = ''
        self.graph_url = 'https://graph.microsoft.com'
        self.tenantGroupName = ''
        self.hostpoolName = ''
        self.appGroupName = ''
        self.grpName = ''

    async def fetch(self, fall_headers, spring_headers, graph_headers,
                    sub_id, appgroups):
        try:
            url_v1 = ["{0}{1}".format(grp.id, '/AssignedUsers')
                      for grp in appgroups if 'rdbroker' in grp.id]
            user_list_v1 = await self.fetch_v1(url_v1, fall_headers)
            atlogger.debug("@PG user_list_v1 fetched successfully")
            user_list_v2, ad_user = await self.fetch_v2(spring_headers, graph_headers,
                                                         sub_id)
            atlogger.debug("@PG user_list_v2 fetched successfully")

            return user_list_v1, user_list_v2, ad_user
        except Exception as e:
            atlogger.debug("@PG exception in fetch %s", e)

    async def parse(self, user_list_v1, user_list_v2, ad_users, appgroups, graph_headers):
        try:
            userassignments = []
            atlogger.debug("entering into parse UserProvisionedDetails")
            for usr in user_list_v1:
                usrassg = UserProvisionedDetails()
                await usrassg.parse_v1(usr)
                userassignments.append(usrassg)
                atlogger.debug("User Assignments V1:%s",userassignments)
    
            for usr in user_list_v2:
                usrassg = UserProvisionedDetails()
                await usrassg.parse_v2(usr, ad_users,appgroups, graph_headers)
                userassignments.append(usrassg)
                atlogger.debug("User Assignments V2:%s",userassignments)
            atlogger.debug("Returning :%s", userassignments)
            return userassignments

        except Exception as e:
            atlogger.debug("@PG exception as %s", e)

    async def fetch_v1(self, urls, headers):
        userInfo = await get_async(urls, headers)
        usr_info = [usr for user in userInfo for usr in user]
        return usr_info

    async def fetch_v2(self, spring_headers, graph_headers, sub_id):
        try:
            async with aiohttp.ClientSession(headers=spring_headers, trust_env=True) as session:
                role_def_id = await self.get_roledefinition_id(session, spring_headers,
                                                               sub_id)
                role_assg = await self.get_roleassignments(session, spring_headers,
                                                           sub_id)
                usrs_prov = [ass for ass in role_assg['value']
                             if role_def_id in ass['properties']['roleDefinitionId']]
                atlogger.debug("User Provisioning Data:%s",usrs_prov)
                """async with aiohttp.ClientSession(headers=graph_headers, trust_env=True) as session:
                    group_url = 'https://graph.microsoft.com/v1.0/groups'
                    atlogger.debug("going to fetch get_details_async group details")
                    grps = await get_details_async(session, graph_headers, group_url)
                    atlogger.debug("value of ad_users %s", grp_usrs)"""
                query = "select * from wvd_users"
                users_dict = await self.sql_executor_parker(query)
                ad_users = users_dict
                

            return usrs_prov, ad_users
        except Exception as e:
            atlogger.debug("@PG exception in fetch_v2 as %s", e)
            return [], [], []

    """ Get role defnition id for Desktop virtualization user role """
    async def get_roledefinition_id(self, session, spring_headers, sub_id):
        role_def_url = 'https://management.azure.com/subscriptions/{0}/'\
            'providers/Microsoft.Authorization/roleDefinitions'.format(sub_id)
        role_def_params = {"api-version": "2018-07-01",
                           "$filter": "roleName eq 'Desktop Virtualization User'"}
        vir_usr = await get_details_async(session, spring_headers,
                                          role_def_url, role_def_params)
        role_def_id = ''.join([role['name'] for role in vir_usr['value']])

        return role_def_id

    async def get_roleassignments(self, session, spring_headers, sub_id):
        role_asg_url = 'https://management.azure.com/subscriptions/{0}/'\
            'providers/Microsoft.Authorization/roleAssignments'.format(sub_id)
        role_asg_params = {"api-version": "2015-07-01"}
        role_assg = await get_details_async(session, spring_headers,
                                            role_asg_url, role_asg_params)
        return role_assg


    async def sql_executor_parker(self, query):
        try:
            conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='postgres', database='anuntatech')
            curObj = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            atlogger.debug(f"Executing --> '{query}'")
            curObj.execute(query)
            users_dict = curObj.fetchall()
            conn.commit()
            curObj.close()
            conn.close()
            return users_dict[0]

        except Exception as e:
            atlogger.debug(f"Caught Execption '{e}' while executing {query}")
            return False

    async def parse_v1(self, assignment):
        self.userPrincipalName = assignment.get('userPrincipalName')
        self.tenantName = assignment.get('tenantName')
        self.tenantGroupName = assignment.get('tenantGroupName')
        self.hostpoolName = assignment.get('hostPoolName')
        self.appGroupName = assignment.get('appGroupName')

    async def parse_v2(self, assignment, ad_users, appgroups, graph_headers):
        try:
            atlogger.debug("Entering into parse v2 user session")
            principalid = assignment.get('properties', {}).get('principalId')
            #name = ''.join([str(usr['userPrincipalName']) for usr in ad_users['value']
            #               if str(principalid) in usr['id']])
            #query = "select * from wvd_users"
            #users_dict = await self.sql_executor_parker(query)
            #self.userPrincipalName = ad_users[0].get('principalid')
            #atlogger.debug(" self.userPrincipalName is %s,%s",self.userPrincipalName,principalid)
            #role_id =  assignment['properties']['roleDefinitionId'].split("/")[-1]
            principalid = [principalid]
            atlogger.debug("principal id list is %s", principalid)
            group_url = self.graph_url + "/v1.0/directoryObjects/getByIds"
            group_body = {
                "ids": principalid,
                "types": ["group","user"],
            }
            groups_headers1=graph_headers
            groups_headers1["Content-Type"] = "application/json"
            user_groups_res = requests.post(group_url, json=group_body, headers=groups_headers1)
            atlogger.debug("@PG status code is %s",user_groups_res.status_code)
            if user_groups_res.status_code == 200:
                user_group_res = user_groups_res.json()
                atlogger.debug("@PG group_res keys are %s",user_group_res.keys())
                if "value" in user_group_res.keys():
                    atlogger.debug("Group response in v2:%s",user_group_res)
                    user_id = [user.get('id') for user in  user_group_res['value'] if user.get('@odata.type')== '#microsoft.graph.user']
                    if len(user_id) > 0:
                        users_url = self.graph_url + "/v1.0/users?$filter = id eq " + str(user_id[0])
                        try:
                            async with aiohttp.ClientSession(headers=graph_headers, trust_env=True) as session:
                                user_res = await get_details_async(session, graph_headers, users_url)
                                for users in user_res.get('value'):
                                    self.userPrincipalName = users.get('displayName')
                        except Exception as e:
                            atlogger.debug("Error occured :%s",e)
                    group_id = [group.get('id') for group in  user_group_res['value'] if group.get('@odata.type')== '#microsoft.graph.group']
                    atlogger.debug("@PG len of group is is %s", len(group_id))
                    if len(group_id) > 0:
                        groups_url = self.graph_url + "/v1.0/groups?$filter = id in " + str(group_id)
                        try:
                            async with aiohttp.ClientSession(headers=graph_headers, trust_env=True) as session:
                                group_res = await get_details_async(session, graph_headers, groups_url)
                                atlogger.debug("Group responses:%s",group_res['value'])
                                for groups in group_res.get('value'):
                                    atlogger.debug("Group id :%s",groups)
                                    self.grpName = groups.get('displayName')
                                atlogger.debug("Group List:%s",self.grpName)
                        except Exception as e:
                            atlogger.debug("Error occured :%s",e)
            atlogger.debug(f"Principal Id {principalid} and User Principal Name {self.userPrincipalName}")
            #self.userPrincipalName = name
            scope = assignment.get('properties', {}).get('scope', "")
            self.hostpoolName = ''
            self.appGroupName = scope.split('/')[-1]
            for grp in appgroups:
                if grp.appGroupName == self.appGroupName:
                    self.hostpoolName = grp.hostPoolName
                    #atlogger.debug("Hostpoolname in springUP: %s", self.hostpoolName)
                    return
        except Exception as e:
            atlogger.debug("Error occured :%s",e)

    def user_to_xml(self):
        #up = xml.Element('user')
        if self.userPrincipalName:
            up = xml.Element('user')
            up.append(xml_tag('userPrincipalName', self.userPrincipalName))
            up.append(xml_tag('tenantName', self.tenantName))
            up.append(xml_tag('tenantGroupName', self.tenantGroupName))
            up.append(xml_tag('hostPoolName', self.hostpoolName))
            up.append(xml_tag('appGroupName', self.appGroupName))
            return up
        return

    def group_to_xml(self):
        #up = xml.Element('group')
        if self.grpName:
            up = xml.Element('group')
            up.append(xml_tag('groupName', self.grpName))
            up.append(xml_tag('tenantName', self.tenantName))
            up.append(xml_tag('tenantGroupName', self.tenantGroupName))
            up.append(xml_tag('hostPoolName', self.hostpoolName))
            up.append(xml_tag('appGroupName', self.appGroupName))
            return up
        return

# @VS ApplicationAndPackageInfo Classes Start
class ApplicationAndPackageInfo:
    def __init__(self, resourcepool_id, subscription_id, management_bearer_token, graph_bearer_token, log_bearer_token, hostpools, applicationgroups, sessionhosts):
        self.msix_package_info = []
        self.msix_application_info = []
        self.published_application_info = []
        self.resourcepool_id = resourcepool_id
        self.subscription_id = subscription_id
        self.management_url = f"https://management.azure.com"
        self.graph_url = f"https://graph.microsoft.com"
        self.management_headers = {
            "Authorization" : f"Bearer {management_bearer_token}"
        }
        self.graph_headers = {
            "Authorization" : f"Bearer {graph_bearer_token}"
        }
        self.log_headers = {
            "Authorization" : f"Bearer {log_bearer_token}"
        }
        self.hostpools = hostpools
        self.applicationgroups = applicationgroups
        self.sessionhosts = sessionhosts
        self.process_names = {}
        self.process_filepath_map = {}

    async def fetch_application_and_package_info(self):
        try:
            msix_app_attach_info = MsixAppAttachInfo(self.subscription_id, self.management_url, self.management_headers, self.hostpools)
            atlogger.info("Fetching MsixPacakges msix_app_attach_info.fetch_msix_packages")
            self.msix_package_info = msix_app_attach_info.fetch_msix_packages()
            atlogger.info("Completed fetching MsixPackages msix_app_attach_info.fetch_msix_packages")
            atlogger.info("Fetching AllApplications self.fetch_applications")
            published_applications, msix_applications = await self.fetch_applications()
            atlogger.info("Completed AllApplications fetching self.fetch_applications")
            atlogger.info("Fetching MsixApplications msix_app_attach_info.fetch_msix_applications")
            self.msix_application_info = msix_app_attach_info.fetch_msix_applications(msix_applications, self.msix_package_info)
            atlogger.info("Completed fetching MsixApplications msix_app_attach_info.fetch_msix_applications")
            atlogger.info("Fetching PublishedApplications self.fetch_published_application_info")
            self.published_application_info = self.fetch_published_application_info(published_applications)
            atlogger.info("Completed fetching PublishedApplications self.fetch_published_application_info")
            atlogger.info("Generating PublishedApp Report")
            appreport = PublishedAppReport(self.resourcepool_id, self.management_headers, self.log_headers, self.subscription_id, self.hostpools, self.sessionhosts, self.process_names, self.process_filepath_map)
            atlogger.info("----- 1 -----")
            appreport.fetch_report()
            atlogger.info("----- 2 -----")
            atlogger.info("Completed generating PublishedApp Report")

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            atlogger.error("Error in ApplicationAndPackageInfo.fetch_application_and_package_info: %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno)))

    def fetch_sessionhosts(self):
        pass

    async def fetch_applications(self):
        published_applications = []
        msix_applications = []

        try:
            api_version = "2022-02-10-preview"
            for applicationgroup in self.applicationgroups:
                url = applicationgroup.id + "/applications?api-version=" + api_version
                res = requests.get(url=url, headers=self.management_headers)
                assigned_users, assigned_groups = await self.fetch_assigned_users(applicationgroup)
                atlogger.debug("Assigned users and groups:%s %s",assigned_users,assigned_groups)
                if res.status_code == 200:
                    res = res.json()
                    if "value" in res.keys():
                        for application in res['value']:
                            application['users'] = assigned_users
                            application['groups'] = assigned_groups
                            application['hostpool'] = applicationgroup.hostpool_armpath.split("/")[-1] if applicationgroup.hostpool_armpath != None else ""
                            application['workspace'] = applicationgroup.workspace_armpath.split("/")[-1] if applicationgroup.workspace_armpath != None else ""
                            if application['properties']['applicationType'] == "MsixApplication":
                                msix_applications.append(application)
                            else:
                                if "filePath" in application['properties'].keys():
                                    filepath = application['properties']['filePath']
                                    if filepath != "":
                                        self.process_filepath_map[".".join(filepath.split("\\")[-1].split(".")[:-1])] = filepath
                                        filepath = ".".join(filepath.split("\\")[-1].split(".")[:-1])
                                        if applicationgroup.hostpool_armpath not in self.process_names.keys():
                                            self.process_names[applicationgroup.hostpool_armpath] = []
                                        self.process_names[applicationgroup.hostpool_armpath].append(filepath)
                                published_applications.append(application)
                else:
                    atlogger.error("Error in API {} {}".format(url, res.text))
        except Exception as e:
            atlogger.error("Error in ApplicationAndPackageInfo.fetch_applications: {}".format(e))

        atlogger.info("self.process_names = {}".format(self.process_names))

        return published_applications, msix_applications

    async def fetch_assigned_users(self, application_group):
        principle_id_list = []
        users_list =[]
        group_list = []
        
        try:
            api_version = "2014-04-01-preview"
            role_assignment_prodiver = "/providers/Microsoft.Authorization/roleAssignments"
            role_assignment_url =  application_group.id + role_assignment_prodiver + "?api-version=" + api_version
            role_assignment_res = requests.get(url=role_assignment_url, headers=self.management_headers)
            role_id = "1d18fff3-a72a-46b5-b4a9-0b38a3cd7e63"
            if role_assignment_res.status_code == 200:
                role_assignment_res = role_assignment_res.json()
                if "value" in role_assignment_res.keys():
                    principle_id_list = [role_assignment['properties']['principalId'] for role_assignment in role_assignment_res['value'] if role_assignment['properties']['roleDefinitionId'].split("/")[-1] == role_id]
            else:
                atlogger.error('Error in API {} : {}'.format(role_assignment_url, role_assignment_res.text))

            if len(principle_id_list) > 0:
                users_url = self.graph_url + "/v1.0/directoryObjects/getByIds"
                atlogger.debug("@PG principle_id_list is %s", principle_id_list)
                users_body = {
                    "ids": principle_id_list,
                    "types": ["user","group"],
                }

                users_groups_headers=self.graph_headers
                users_groups_headers["Content-Type"] = "application/json"
                users_groups_res = requests.post(users_url, json=users_body, headers=users_groups_headers)
                if users_groups_res.status_code == 200:
                    users_groups_res = users_groups_res.json()
                    if "value" in users_groups_res.keys():
                        atlogger.debug("Group response :%s",users_groups_res)
                        group_id = [user_and_group.get('id') for user_and_group in  users_groups_res['value'] if user_and_group.get('@odata.type')== '#microsoft.graph.group']
                        atlogger.debug("Group Id is :%s",group_id)
                        users_list = [user_and_group for user_and_group in users_groups_res['value'] if user_and_group.get('@odata.type')== '#microsoft.graph.user']
                        if len(group_id) > 0:
                            groups_url = self.graph_url + "/v1.0/groups?$filter = id in " + str(group_id)
                            try:
                                async with aiohttp.ClientSession(headers=users_groups_headers, trust_env=True) as session:
                                    group_res = await get_details_async(session, users_groups_headers, groups_url)
                                    atlogger.debug("Group response:%s",group_res['value'])
                                    group_list = [groups for groups in group_res.get('value')]
                                    atlogger.debug("Group List:%s",group_list)
                            except Exception as e:
                                atlogger.error("Error exception {}".format(e))

                else:
                    atlogger.error('Error in API {} : {}'.format(users_url, users_res.text))
        except Exception as e:
            atlogger.error("Error in ApplicationAndPackageInfo.fetch_assigned_users: {}".format(e))

        return users_list, group_list

    def fetch_published_application_info(self, published_applications):
        pa_obj_list = []
        
        try:
            for published_application in published_applications:
                pa_obj = PublishedApp()
                pa_obj.pool_name = published_application['hostpool']
                pa_obj.workspace_name = published_application['workspace']
                pa_obj.application_name = published_application['name'].split("/")[-1]
                atlogger.info("APPLICATIONNAME = {}".format(published_application['properties']['filePath'].split("\\")[-1]))
                pa_obj.application_exe = published_application['properties']['filePath'].split("\\")[-1]
                pa_obj.application_group = published_application['name'].split("/")[0]
                users = []
                groups = []
                atlogger.debug("Application Response:%s",published_application)
                for user in published_application['users']:
                    user_obj = User()
                    user_obj.user_principal_name = user['userPrincipalName']
                    user_obj.display_name = user['displayName']
                    users.append(user_obj)
                pa_obj.user_info = users
                for group in published_application['groups']:
                    group_obj = Group()
                    group_obj.display_name = group['displayName']
                    groups.append(group_obj)
                pa_obj.group_info = groups
                pa_obj_list.append(pa_obj)
        except Exception as e:
            atlogger.error("Error in ApplicationAndPackageInfo.fetch_published_application_info: {}".format(e))

        return pa_obj_list

    def generate_report(self):
        
        pass

    def to_xml(self):
        application_and_package_info = xml.Element('applicationAndPackageInfo')
        
        try:
            published_app_info = xml.Element('publishedAppInfo')
            for published_app in self.published_application_info:
                published_app_info.append(published_app.to_xml())
            application_and_package_info.append(published_app_info)

            msix_app_attach_info = xml.Element('msixAppAttachInfo')
            msix_package_info = xml.Element('msixPackageInfo')
            for msix_package in self.msix_package_info:
                msix_package_info.append(msix_package.to_xml())
            msix_app_attach_info.append(msix_package_info)

            msix_application_info = xml.Element('msixApplicationInfo')
            for msix_application in self.msix_application_info:
                msix_application_info.append(msix_application.to_xml())
            msix_app_attach_info.append(msix_application_info)
            application_and_package_info.append(msix_app_attach_info)
        
            application_and_package_info_xml_string = xml.tostring(application_and_package_info, encoding="utf-8", pretty_print=True).decode("utf-8")
        except Exception as e:
            atlogger.error("Error in ApplicationAndPackageInfo.to_xml: {}".format(e))

        return application_and_package_info

class MsixAppAttachInfo:
    def __init__(self, subscription_id, management_url, management_headers, hostpools):
        self.subscription_id = subscription_id
        self.management_url = management_url
        self.management_headers = management_headers
        self.hostpools = hostpools

    def fetch_msix_packages(self):
        msix_packages = []
        ma_obj_list = []

        try:
            api_version = "2022-02-10-preview"
            for hostpool in self.hostpools:
                url = self.management_url + hostpool.id + "/msixpackages?api-version=" + api_version
                res = requests.get(url=url, headers=self.management_headers)
                if res.status_code == 200:
                    res = res.json()
                    if "value" in res:
                        msix_packages.extend(res['value'])
                else:
                    atlogger.error("Error in API {} : {}".format(url, res.text))
            for msix_package in msix_packages:
                msix_package_size = self.fetch_msix_package_size(msix_package['properties']['imagePath'])
                for application in msix_package['properties']['packageApplications']:
                    mp_obj = MsixPackage()
                    mp_obj.package_name = msix_package['name'].split("/")[-1]
                    mp_obj.version = msix_package['properties']['version']
                    mp_obj.application_name = application['friendlyName']
                    mp_obj.status = "Active" if msix_package['properties']['isActive'] else "Inactive"
                    mp_obj.registration_type = "Log on blocking" if msix_package['properties']['isRegularRegistration'] else "On-demand"
                    mp_obj.package_family_name = msix_package['properties']['packageFamilyName']
                    if len(msix_package['properties']['packageDependencies']) > 0:
                        mp_obj.publisher = msix_package['properties']['packageDependencies'][0]['publisher']
                    mp_obj.size = msix_package_size
                    ma_obj_list.append(mp_obj)
        except Exception as e:
            atlogger.error("Error in MsixAppAttachInfo.fetch_msix_packages: {}".format(e))

        return ma_obj_list

    def fetch_msix_package_size(self, image_path):
        size = ""
        size_format = "bytes"

        try:
            image_path_parts = [x for x in image_path.split('\\') if x!='']
            image_path = '/'.join(image_path_parts)
            account_name = image_path_parts[0].split('.')[0]

            storage_account_keys = self.get_storage_account_keys(account_name)

            for account_key in storage_account_keys:
                url = 'https://'+ image_path
                headers = {
                    'x-ms-date': datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),
                    'x-ms-version': '2018-03-28',
                }
                canonicalized_headers = '\n'.join(f'{key}:{value}' for key, value in sorted(headers.items()))
                canonicalized_resource = f'/{account_name}/{"/".join(image_path_parts[1:])}'
                string_to_sign = f'HEAD\n\n\n\n\n\n\n\n\n\n\n\n{canonicalized_headers}\n{canonicalized_resource}'
                signed_key = base64.b64encode(hmac.new(base64.b64decode(account_key), string_to_sign.encode('utf-8'), hashlib.sha256).digest())
                headers['Authorization'] = f'SharedKey {account_name}:{signed_key.decode("utf-8")}'
                res = requests.head(url, headers=headers)
                if res.status_code == 200:
                    size = int(res.headers['Content-Length'])
                    if size > 1000:
                        size = (size / 1024)
                        size_format = "KiB"
                        if size > 1000:
                            size = (size / 1024)
                            size_format = "MiB"
                            if size > 1000:
                                size = (size / 1024)
                                size_format = "GiB"
                                if size > 1000:
                                    size = (size / 1024)
                                    size_format = "TiB"
                    break
                else:
                    atlogger.error("Error in API {} : {}".format(url, res.text))

        except Exception as e:
            atlogger.error("Error in MsixAppAttachInfo.fetch_msix_package_size: {}".format(e))
            
        size = f"{round(size, 1)} {size_format}"
        return size

    def get_storage_account_keys(self, account_name):
        keys = []
        try:
            conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='postgres', database='anuntatech')
            curObj = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = f"SELECT id FROM azresources WHERE text(type) = '\"Microsoft.Storage/storageAccounts\"' AND text(name) = '\"{account_name}\"'"
            curObj.execute(query)
            res = curObj.fetchall()
            conn.close()
            if len(res) > 0:
                storage_account_id = res[0][0]
                api_version = "2019-06-01"
                url = self.management_url + storage_account_id + "/listKeys?api-version=" + api_version
                res = requests.post(url=url, headers=self.management_headers)

                if res.status_code == 200:
                    res = res.json()
                    if "keys" in res:
                        keys = [key['value'] for key in res['keys']]
                else:
                    atlogger.error("Error in API {} : {}".format(url, res.text))
        except Exception as e:
            atlogger.error("Error in MsixAppAttachInfo.get_storage_account_keys: {}".format(e))

        return keys

    def fetch_msix_applications(self, msix_applications, msix_packages):
        ma_obj_list = []

        try:
            for msix_application in msix_applications:
                msix_package = next((msix_package for msix_package in msix_packages if msix_package.package_family_name == msix_application['properties']['msixPackageFamilyName']), None)
                ma_obj = MsixApplication()
                ma_obj.pool_name = msix_application['hostpool']
                ma_obj.workspace_name = msix_application['workspace']
                ma_obj.application_name = msix_application['name'].split("/")[-1]
                ma_obj.application_group = msix_application['name'].split("/")[0]
                ma_obj.package_name = msix_package.package_name
                ma_obj.publisher = msix_package.publisher

                users = []
                for user in msix_application['users']:
                    user_obj = User()
                    user_obj.user_principal_name = user['userPrincipalName']
                    user_obj.display_name = user['displayName']
                    users.append(user_obj)
                ma_obj.user_info = users
                ma_obj_list.append(ma_obj)
        except Exception as e:
            atlogger.error("Error in MsixAppAttachInfo.fetch_msix_applications: {}".format(e))

        return ma_obj_list

class MsixPackage:
    def __init__(self):
        self.package_name = ""
        self.version = ""
        self.application_name = ""
        self.status = ""
        self.size = ""
        self.registration_type = ""
        self.publisher = ""
        self.package_family_name = ""

    def to_xml(self):
        msix_package = xml.Element("msixPackage")

        msix_package.append(xml_tag("packageName", self.package_name))
        msix_package.append(xml_tag("version", self.version))
        msix_package.append(xml_tag("applicationName", self.application_name))
        msix_package.append(xml_tag("status", self.status))
        msix_package.append(xml_tag("size", self.size))
        msix_package.append(xml_tag("registrationType", self.registration_type))

        return msix_package

class MsixApplication:
    def __init__(self):
        self.pool_name = ""
        self.workspace_name = ""
        self.application_name = ""
        self.application_exe = ""
        self.application_group = ""
        self.publisher = ""
        self.package_name = ""
        self.user_info = []

    def to_xml(self):
        msix_application = xml.Element("msixApplication")
        msix_application.append(xml_tag("poolName", self.pool_name))
        msix_application.append(xml_tag("applicationName", self.application_name))
        msix_application.append(xml_tag("publisher", self.publisher))
        msix_application.append(xml_tag("packageName", self.package_name))

        user_info = xml.Element("userInfo")
        for user in self.user_info:
            user_info.append(user.to_xml())
        msix_application.append(user_info)

        return msix_application

class PublishedApp:
    def __init__(self):
        self.pool_name = ""
        self.workspace_name = ""
        self.application_name = ""
        self.application_exe = ""
        self.application_group = ""
        self.user_info = []
        self.group_info = []

    def to_xml(self):
        atlogger.debug("Entering to generate xml")
        published_app = xml.Element("publishedApp")
        published_app.append(xml_tag("poolName", self.pool_name))
        published_app.append(xml_tag("applicationName", self.application_exe))

        user_info = xml.Element("userInfo")
        group_info = xml.Element("groupInfo")
        for user in self.user_info:
            user_info.append(user.to_xml())
        for group in self.group_info:
            group_info.append(group.to_xml())
        published_app.append(user_info)
        published_app.append(group_info)

        return published_app

class User:
    def __init__(self):
        self.user_principal_name = ""
        self.display_name = ""

    def to_xml(self):
        user = xml.Element("user")
        user.append(xml_tag("name", self.display_name))

        return user

class Group:
    def __init__(self):
        self.display_name = ""

    def to_xml(self):
        group = xml.Element("group")
        group.append(xml_tag("name", self.display_name))

        return group

class PublishedAppReport:
    def __init__(self, resourcepool_id, management_headers, log_headers, subscription_id, hostpools, sessionhosts, process_names, process_filepath_map):
        self.resourcepool_id = resourcepool_id
        self.management_headers = management_headers
        self.log_headers = log_headers
        self.subscription_id = subscription_id
        self.hostpools = hostpools
        self.sessionhosts = sessionhosts
        self.workspace_ids = []
        self.current_sessions = []
        self.process_names = process_names
        self.process_filepath_map = process_filepath_map
        self.workspace_ids = self.fetch_workspace_ids()
        self.ape_result = []
        self.report_rows = []
        self.report_dict = {}

    def fetch_report(self):
        #self.fetch_from_pwsh()
        atlogger.debug("@A APE Result: %s", self.ape_result)

        for res in self.ape_result:
            atlogger.info("res = {}".format(res))
            if "user" not in res.keys():
                continue
            elif res["user"].strip("\n").strip(" ") in ["", "NotFound", "Unknown", "Error"]:
                continue

            if res["type"] == "started":
                new_process = True
                
                current_query = "SELECT * FROM apppeakconcurrency WHERE app_name= '{}' and username= '{}' and resourcepoolid= '{}' ORDER BY logintime DESC LIMIT 1"
                current_query = current_query.format(res["application"]+".exe", res["user"], self.resourcepool_id)
                atlogger.info("current_query = {}".format(current_query))
                current_query_resp = execute_query(current_query)
                atlogger.info("current_query_resp = {}".format(current_query_resp))

                if len(current_query_resp) > 0:
                    current_launch_end = current_query_resp[0][7]
                    if current_launch_end == None:
                        new_process = False
                else:
                    historic_query = "SELECT * FROM historicapppeakconcurrency WHERE app_name = '{}' and username = '{}' and resourcepoolid = '{}' ORDER BY logintime DESC LIMIT 1"
                    historic_query = historic_query.format(res["application"]+".exe", res["user"], self.resourcepool_id)
                    atlogger.info("historic_query = {}".format(historic_query))
                    historic_query_resp = execute_query(historic_query)
                    atlogger.info("historic_query_resp = {}".format(historic_query_resp))
                    if len(historic_query_resp) > 0:
                        launch_end = historic_query_resp[0][7]
                        if launch_end == None:
                            new_process = False

                    atlogger.info("new_process = {}".format(new_process))

                if new_process:
                    insert_query = "INSERT INTO apppeakconcurrency(resourcepoolid, app_name, app_path, username, usage, logintime, disconnecttime, connection_type, connection_type_value, status) VALUES('{}', '{}', '{}', '{}', {}, {}, {}, '{}', '{}', 'active')"
                    insert_query = insert_query.format(self.resourcepool_id, res["application"]+".exe", self.process_filepath_map[res["application"]], res["user"] if "user" in res.keys() else "", 'NULL', f"'{res['timestamp']}'" if res["type"]=="started" else 'NULL', f"'{res['timestamp']}'" if res["type"]=="ended" else 'NULL', "SessionHost", res["sessionhost"])
                    atlogger.info("fetch_report insert_query = {}".format(insert_query))
                    execute_query(insert_query)
            elif res["type"] == "ended":
                atlogger.info("Entering ended res = {}".format(res))
                current_query = "SELECT * FROM apppeakconcurrency WHERE app_name = '{}' and username = '{}' and resourcepoolid = '{}' ORDER BY logintime DESC LIMIT 1"
                current_query = current_query.format(res["application"]+".exe", res["user"], self.resourcepool_id)
                atlogger.info("current_query_ended = {}".format(current_query))
                current_query_resp = execute_query(current_query)
                atlogger.info("current_query_resp_ended = {}".format(current_query_resp))
                
                if len(current_query_resp) != 0:
                    current_row_id = current_query_resp[0][0]
                    current_row_launch_start = current_query_resp[0][6]
                    launch_end = f"{res['timestamp']}"
                    launch_end = datetime.strptime(launch_end, '%m/%d/%Y %H:%M:%S')

                    current_launch_end = current_query_resp[0][7]

                    if current_launch_end == None:
                        usage_str = ""
                        try:
                            usage = (launch_end - current_row_launch_start).total_seconds()
                            usage_hh = int(usage // 3600)
                            usage_mm = int((usage % 3600) // 60)
                            usage_ss = int(usage % 60)
                            usage_str = f"{usage_hh}:{usage_mm}"
                        except Exception as e:
                            atlogger.error("Exception in usage: {}".format(e))

                        update_query = "UPDATE apppeakconcurrency set disconnecttime = {}, usage = '{}', status = 'disconnect' WHERE id = '{}' and resourcepoolid = '{}'"
                        update_query = update_query.format(f"'{res['timestamp']}'" if res["type"]=="ended" else 'NULL', usage_str, current_row_id, self.resourcepool_id)
                        atlogger.info("update_query = {}".format(update_query))
                        execute_query(update_query)
                else:
                    historic_query = "SELECT * FROM historicapppeakconcurrency WHERE app_name = '{}' and username = '{}' and resourcepoolid ='{}' ORDER BY logintime DESC LIMIT 1"
                    historic_query = historic_query.format(res["application"]+".exe", res["user"], self.resourcepool_id)
                    atlogger.info("historic_query = {}".format(historic_query))
                    historic_query_resp = execute_query(historic_query)
                    atlogger.info("historic_query_resp = {}".format(historic_query_resp))
                    if len(historic_query_resp) != 0:
                        historic_row = historic_query_resp[0]
                        historic_row_id = historic_row[0]
                        historic_row_user = historic_row[4]
                        historic_row_launch_start = historic_row[6]

                        history_launch_end = historic_row[7]

                        if history_launch_end == None:
                            launch_end = f"{res['timestamp']}"
                            launch_end = datetime.strptime(launch_end, '%m/%d/%Y %H:%M:%S')

                            usage_str = ""
                            try:
                                usage = (launch_end - historic_row_launch_start).total_seconds()
                                usage_hh = int(usage // 3600)
                                usage_mm = int((usage % 3600) // 60)
                                usage_ss = int(usage % 60)
                                usage_str = f"{usage_hh}:{usage_mm}"
                            except Exception as e:
                                atlogger.error("Exception in usage: {}".format(e))
 
                            insert_query = "INSERT INTO apppeakconcurrency(id, resourcepoolid, app_name, app_path, username, usage, logintime, disconnecttime, connection_type, connection_type_value, status) VALUES('{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', 'disconnect')"
                            insert_query = insert_query.format(historic_row_id, self.resourcepool_id, res["application"]+".exe", self.process_filepath_map[res["application"]], historic_row_user, usage_str, historic_row_launch_start, f"'{res['timestamp']}'" if res["type"]=="ended" else 'NULL', "SessionHost", res["sessionhost"])
                            atlogger.info("insert_query2 = {}".format(insert_query))
                            execute_query(insert_query)

            """insert_query = "INSERT INTO apppeakconcurrency(id, resourcepoolid, app_name, app_path, username, usage, launch_start, launch_end, connection_type, connection_type_value) VALUES('{}', '{}', '{}', '{}', '{}', {}, {}, {}, '{}', '{}')"
            insert_query = insert_query.format("123", self.resourcepool_id, res["application"], "/", res["user"] if "user" in res.keys() else "", 'NULL', f"'{res['timestamp']}'" if res["type"]=="started" else 'NULL', f"'{res['timestamp']}'" if res["type"]=="ended" else 'NULL', "SessionHost", res["sessionhost"])
            atlogger.info("fetch_report insert_query = {}".format(insert_query))
            execute_query(insert_query)"""
    
        """with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.fetch_published_app_report, hostpool.id) for hostpool in self.hostpools]
            concurrent.futures.wait(futures)
        
        atlogger.debug("@VS self.report_rows = {}".format(self.report_rows))
        for row in self.report_rows:
            try:
                atlogger.debug("@VS row in self.report_rows = {}".format(row))
                query = "INSERT INTO apppeakconcurrency(id, resourcepoolid, app_name, app_path, username, usage, launch_start, launch_end, connection_type, connection_type_value) VALUES('{}', '{}', '{}', '{}', '{}', '{}', {}, {}, '{}', '{}')"
                query = query.format(row["correlation_id"], self.resourcepool_id, row["application_name"], row["folder_path"], row["username"], row["usage"], f"'{row['started']}'" if row["started"] != None else "NULL", f"'{row['completed']}'" if row["completed"] != None else "NULL", row["type"], row["sessionhost"])
                query += " ON CONFLICT (id) DO UPDATE SET launch_end = EXCLUDED.launch_end, usage = EXCLUDED.usage"
                atlogger.info("@VS query = {}".format(query))
                execute_query(query)
            except Exception as e:
                atlogger.error("Error in fetch_app_report = {}".format(e))"""

    def fetch_from_pwsh(self):
        atlogger.info("Entering PWSH:")
        APE = AzurePowershellExecutor(self.management_headers)
        
        vm_app_map = self.get_vm_app_map()
        
        fetch_monitoring_script = APE.create_script(filepath="/root/anuntatech3/src/AzureMonitoring/ps1_scripts/fetchProcessMonitoring.ps1")
        def fetch_process_monitoring(vm):
            vm = vm["vmId"]
            atlogger.info("Entering collect_data for {}".format(vm.split("/")[-1]))
            std_out, std_err, logs = APE.execute_and_fetch_output(vm, fetch_monitoring_script)
            atlogger.info("Completed APE.execute_and_fetch_output for {}".format(vm.split("/")[-1]))
            atlogger.info("RES for {} : {}".format(vm.split("/")[-1], std_out))
            if std_out!= None:
                out = std_out["message"]
                res = [line.strip("\n") for line in out.split("\n")]
                dicts = []

                atlogger.info("fetch_monitoring_script_result = {}".format(res))
                try:
                    for row in res:
                        if "PROCESS MONITORING" in row:
                            continue
                        temp_row = row.split(",")
                        if len(temp_row) != 5:
                            continue
                        try:
                            row_dict = dict()
                            row_val = row.split(",")
                            atlogger.info("row_val = {}".format(row_val))
                            atlogger.info("len of row_val = {}".format(len(row_val)))
                            atlogger.info("row_val[0] = {}".format(row_val[0]))
                            row_dict["timestamp"] = row_val[0]
                            row_dict["sessionhost"] = row_val[1]
                            row_dict["type"] = row_val[2].lower()
                            row_dict["application"] = row_val[3]
                            row_dict["user"] = row_val[4]
                            atlogger.info("row_dict = {}".format(row_dict))
                            dicts.append(row_dict)
                        except Exception as e:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            atlogger.error("Error in row parsing as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno)))
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    atlogger.error("Error in message parsing as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno)))

                self.ape_result.extend(dicts) 
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            fetch_monitoring_futures = [executor.submit(fetch_process_monitoring, vm) for vm in vm_app_map]
            concurrent.futures.wait(fetch_monitoring_futures)

        mins = 27
        def start_process_monitoring(vm):
            process_names = "{}".format(tuple(vm["apps"])).replace("'", '"')
            start_monitoring_script = APE.create_script(filepath="/root/anuntatech3/src/AzureMonitoring/ps1_scripts/startProcessMonitoring.ps1", formatters=[process_names, vm["vmId"].split("/")[-1],  60*mins])
            atlogger.info("vm_app_map {} {}".format(vm["vmId"].split("/")[-1], process_names))
            APE.execute(vm["vmId"], start_monitoring_script)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            start_monitoring_futures = [executor.submit(start_process_monitoring, vm) for vm in vm_app_map]
            concurrent.futures.wait(start_monitoring_futures)

    def get_vm_app_map(self):
        vm_app_map = []
        vms = []

        for sessionhost in self.sessionhosts:
            if sessionhost.vmResourceId != '' and sessionhost.status == "Available":
                vm_dict = dict()
                vm_dict["resourceId"] = sessionhost.vmResourceId
                vm_dict["hostpool"] = sessionhost.hostPoolResourceId

                vms.append(vm_dict)
        
        for vm in vms:
            vm_app_dict = dict()
            vm_app_dict["vmId"] = vm["resourceId"]
            vm_app_dict["apps"] = self.process_names[vm["hostpool"]] if vm["hostpool"] in self.process_names.keys() else []
            vm_app_map.append(vm_app_dict)

        atlogger.info("vm_app_dict = {}".format(vm_app_dict))
        
        """vmid_query = "SELECT resourceid FROM vmidresourcemap WHERE vmid IN {}"
        vmid_query = vmid_query.format(tuple(sessionhost_ids))
        vm_resource_ids = [vmrid[0] for vmrid in execute_query(vmid_query)]
        atlogger.info("vm_resource_ids = {}".format(vm_resource_ids))

        return vm_resource_ids"""

        return vm_app_map

    def fetch_published_app_report(self, hostpool_id):
        KQL_query = '''
            set query_take_max_records=10001;
            set truncationmaxsize=67108864;
            WVDConnections
            | where  _ResourceId =~ '{0}'\
            | as Connections
            | where  TimeGenerated <= now() and TimeGenerated >= ago(30min)
            | project  CorrelationId, Time=TimeGenerated, UserName, State, SessionHostName
            | join kind=inner(
                WVDCheckpoints
                | where (Name ==  "LaunchExecutable" and Parameters.connectionStage == "RdpShellAppExecuted") or Name=="RdpShellAppExecuted"
                | project CorrelationId, FileName=tostring(Parameters.filename), ActivityType=ActivityType, Type=Type
                | extend FileName=trim('"', FileName)
                | extend FilePath=FileName
                | extend FileName=replace(@\"^(.*[\\\\\\/])\", \"\", tolower(FileName))
                | extend FileName=replace(@\"microsoft.windows(.+)_8wekyb3d8bbwe!app\", @\"\\1\", FileName)) on CorrelationId
                | project  Time, UserName, State, CorrelationId, FilePath, FileName, SessionHostName'''
        
        KQL_query = KQL_query.format(hostpool_id)

        params = {"api-version": "2017-01-01-preview"}
        body = {"query": KQL_query}
        res_rows = []

        try:
            atlogger.info("@VS self.workspace_ids = {}".format(self.workspace_ids))
            for workspace_id in self.workspace_ids:
                url = "https://api.loganalytics.io/v1/workspaces/{}/query"
                url = url.format(workspace_id)
                res = requests.post(url, json=body, headers=self.log_headers, params=params)
                if res.status_code == 200:
                    res = res.json()
                    if "tables" in res.keys():
                        if len(res["tables"]) > 0:
                                if "rows" in res["tables"][0].keys():
                                    res_rows.extend(res["tables"][0]["rows"])
                else:
                    atlogger.error("Error in API {} {}".format(url, res.text))
        except Exception as e:
            atlogger.error("@VS Work error: {}".format(e))

        temp_dict = {}

        try:
            for i in res_rows:
                filename = i[5]
                folder_path = str("\\".join(i[4].split("\\")[:-1])+"\\")
                correlation_id_original = i[3]
                correlation_id = i[3]+filename
                state = i[2].lower()
                time = i[0]
                username = i[1]
                sessionhost = i[6]
                connection_type = "RDSH" if sessionhost == "<>" else "SessionHost"

                if filename not in temp_dict.keys():
                    temp_dict[filename] = {}
                    temp_dict[filename]["instances"] = {}
                    temp_dict[filename]["folder_path"] = folder_path
                if correlation_id not in temp_dict[filename]["instances"].keys():
                    temp_dict[filename]["instances"][correlation_id] = {
                        "timestamps": {},
                        "username": username
                    }
                    temp_dict[filename]["instances"][correlation_id]["sessionhost"] = sessionhost
                    temp_dict[filename]["instances"][correlation_id]["type"] = connection_type
                if state not in temp_dict[filename]["instances"][correlation_id]["timestamps"].keys():
                    temp_dict[filename]["instances"][correlation_id]["timestamps"][state] = time
                    if "completed" in temp_dict[filename]["instances"][correlation_id]["timestamps"].keys() and "started" in temp_dict[filename]["instances"][correlation_id]["timestamps"].keys():
                        atlogger.debug("@VS temp_dict2 = {}".format(temp_dict))
                        started_time = datetime.fromisoformat(temp_dict[filename]["instances"][correlation_id]["timestamps"]["started"][:26])
                        completed_time = datetime.fromisoformat(temp_dict[filename]["instances"][correlation_id]["timestamps"]["completed"][:26])
                        usage = (completed_time - started_time).total_seconds()
                        usage_hh = int(usage // 3600)
                        usage_mm = int((usage % 3600) // 60)
                        usage_ss = int(usage % 60)
                        #usage = f"{usage_hh}:{usage_mm}:{usage_ss}"
                        #usage = f"{usage_mm + (usage_hh*60)}:{usage_ss}"
                        usage = f"{usage_hh}:{usage_mm}"
                        atlogger.debug("@VS usage2 = {}".format(usage))
                        temp_dict[filename]["instances"][correlation_id]["usage"] = str(usage)
        except Exception as e:
            atlogger.error("@VS Error res_rows: {}".format(e))
        
        atlogger.debug("@VS out_temp_dict = {}".format(json.dumps(temp_dict, indent=2)))

        try:
            for app, app_data in temp_dict.items():
                atlogger.debug("@VS app, app_data in temp_dict.items() = {} {}".format(app, app_data))
                for instance, instance_data in app_data["instances"].items():
                    historic_session = self.get_historic_session(instance)
                    row_dict = dict()
                    row_dict["correlation_id"] = instance
                    row_dict["application_name"] = app
                    row_dict["folder_path"] = app_data["folder_path"]
                    row_dict["started"] = None
                    row_dict["completed"] = None
                    complete_row = False
                    if "timestamps" in instance_data.keys():
                        if "started" in instance_data["timestamps"].keys():
                            row_dict["started"] = instance_data["timestamps"]["started"]
                        if "completed" in instance_data["timestamps"].keys():
                            row_dict["completed"] = instance_data["timestamps"]["completed"]
                        if "started" in instance_data["timestamps"].keys() and "completed" in instance_data["timestamps"].keys():
                            if instance_data["timestamps"]["started"] != None and instance_data["timestamps"]["completed"] != None:
                                complete_row = True

                    row_dict["username"] = instance_data["username"]
                    row_dict["usage"] = instance_data["usage"] if "usage" in  instance_data.keys() else None
                    row_dict["sessionhost"] = instance_data["sessionhost"]
                    row_dict["type"] = instance_data["type"]
                    if not complete_row:
                        if "started" in instance_data["timestamps"].keys() and historic_session != None:
                            atlogger.debug("@VS start historic_session = {}".format(historic_session))
                            continue
                        elif "completed" in instance_data["timestamps"].keys() and historic_session != None:
                            row_dict["started"] = historic_session["started"].isoformat()
                        elif "completed" in instance_data["timestamps"].keys() and historic_session == None:
                            from_log = self.get_not_in_historic(hostpool_id, instance[:-len(app)])
                            if from_log != None:
                                row_dict["started"] = from_log
                    if row_dict["started"] != None and row_dict["completed"] != None:
                        atlogger.debug("@VS row_dict['started'] = {} {}".format(row_dict["started"], str(row_dict["started"])))
                        started_time = datetime.fromisoformat(row_dict["started"][:26])
                        completed_time = datetime.fromisoformat(row_dict["completed"][:26])
                        usage = (completed_time - started_time).total_seconds()
                        usage_hh = int(usage // 3600)
                        usage_mm = int((usage % 3600) // 60)
                        usage_ss = int(usage % 60)
                        #usage = f"{usage_hh}:{usage_mm}:{usage_ss}"
                        #usage = f"{usage_mm + (usage_hh*60)}:{usage_ss}"
                        usage = f"{usage_hh}:{usage_mm}"
                        row_dict["usage"] = str(usage)
                    atlogger.info("@VS row_dict = {}".format(json.dumps(row_dict, indent=2)))
                    self.report_rows.append(row_dict)
        except Exception as e:
            atlogger.error("@VS Error in row_dict = {}".format(e))

    def get_historic_session(self, correlation_id):
        query = "SELECT launch_start FROM historicapppeakconcurrency WHERE id = '{}' LIMIT 1"
        query = query.format(correlation_id)
        try:
            session = execute_query(query)
            if session != False and len(session)> 0:
                atlogger.debug("@VS session = {}".format(session))
                session = session[0]
                session_dict = dict()
                session_dict["started"] = session[0]
                return session_dict
            else:
                return None
        except Exception as e:
            atlogger.error("Error in get_historic_session = {}".format(e))
            return None

    def get_not_in_historic(self, hostpool_id, correlation_id):
        KQL_query = '''
            set query_take_max_records=10001;
            set truncationmaxsize=67108864;
            WVDConnections
            | where  _ResourceId =~ '{0}'\
            | as Connections
            | where  TimeGenerated <= now() and CorrelationId == "{1}" and State == "Started"
            | project  CorrelationId, Time=TimeGenerated, UserName, State, SessionHostName
            | join kind=inner(
                WVDCheckpoints
                | where (Name ==  "LaunchExecutable" and Parameters.connectionStage == "RdpShellAppExecuted") or Name=="RdpShellAppExecuted"
                | project CorrelationId, FileName=tostring(Parameters.filename), ActivityType=ActivityType, Type=Type
                | extend FileName=trim('"', FileName)
                | extend FilePath=FileName
                | extend FileName=replace(@\"^(.*[\\\\\\/])\", \"\", tolower(FileName))
                | extend FileName=replace(@\"microsoft.windows(.+)_8wekyb3d8bbwe!app\", @\"\\1\", FileName)) on CorrelationId
                | project  Time'''

        KQL_query = KQL_query.format(hostpool_id, correlation_id)
        atlogger.info("Query is {}".format(KQL_query))

        params = {"api-version": "2017-01-01-preview"}
        body = {"query": KQL_query}
        res_rows = []
        time = None
        
        try:
            for workspace_id in self.workspace_ids:
                url = "https://api.loganalytics.io/v1/workspaces/{}/query"
                url = url.format(workspace_id)
                res = requests.post(url, json=body, headers=self.log_headers, params=params)
                if res.status_code == 200:
                    res = res.json()
                    atlogger.info("NOt in hist json: {}".format(res))
                    if "tables" in res.keys():
                        if len(res["tables"]) > 0:
                                if "rows" in res["tables"][0].keys():
                                    res_rows.extend(res["tables"][0]["rows"])
                else:
                    atlogger.error("Error in API {} {}".format(url, res.text))
        except Exception as e:
            atlogger.error("@VS Work error: {}".format(e))

        if len(res_rows) > 0:
            if len(res_rows[0]) > 0:
                time = res_rows[0][0]
        
        return time

    def fetch_workspace_ids(self):
        query = "SELECT id FROM azresources WHERE TEXT(type)='\"Microsoft.Insights/dataCollectionRules\"'"
        rows = execute_query(query)
        if not((rows == False) and (len(rows)) <= 0):
            self.urls=[row[0] for row in rows if len(row) > 0]
            self.urls = list(set(self.urls))
        else:
            self.urls = []

        workspace_ids = []

        def get_workspace_id(url):
            workspace_headers = self.management_headers
            workspace_headers["Content-Type"] = "application/json"
            workspace_params = {"api-version": "2021-03-01"}
            workspace_body = {
            "query": f"resources\r\n| where id =~ \"{url.lower()}\"\r\n| project properties.destinations.logAnalytics[0].workspaceResourceId, selected=1",
            "options": {
                "resultFormat": "table"
                }
            }
            workspace_url = "https://management.azure.com/providers/microsoft.resourcegraph/resources"
            workspace_res = requests.post(workspace_url,
                                            json=workspace_body,
                                            headers=workspace_headers,
                                            params=workspace_params)
            
            workspace = [[]]
            if workspace_res.status_code == 200:
                workspace_res = workspace_res.json()
                if "data" in workspace_res:
                    if "rows" in workspace_res['data']:
                        workspace = workspace_res['data']['rows']

            if len(workspace) > 0: 
                if len(workspace[0])>0:
                    insights_url = workspace[0][0]
                    url = url.split("/")

                    workspace_id_params = {"api-version":"2023-09-01"}
                    workspace_id_res = requests.get(f"https://management.azure.com{insights_url}",
                                                headers=self.management_headers,
                                                params=workspace_id_params).json()
                    return workspace_id_res['properties']['customerId']


        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(get_workspace_id, url) for url in self.urls]
            concurrent.futures.wait(futures)

            for future in concurrent.futures.as_completed(futures):
                try:
                    workspace_id = future.result()
                    workspace_ids.append(workspace_id)
                except Exception as e:
                    atlogger.error('Generated an exception: {}'.format(e))

        workspace_ids = list(set(workspace_ids))
        workspace_ids = [workspace_id for workspace_id in workspace_ids if workspace_id is not None]
        return workspace_ids

def xml_tag22(tag_name, value=None):
    xml_tag = xml.Element(tag_name)
    if value != None:
        if value.__class__.__name__ == "list":
            for val in value:
                if val.__class__.__name__ == "_Element":
                    xml_tag.append(val)
                else:
                    xml_tag.text = str(val)
        elif value.__class__.__name__ == "_Element":
            xml_tag.append(value)
        else:
            xml_tag.text = str(value)
    return xml_tag
# @VS ApplicationAndMsixPackage Classes End


async def discover_infra_wvddetails(tenantname, tenantid, clientid, clientsecret,subid, tenantmon_flag,productType,customerId,citrixId,citrixSecret,hzurl, hzoauthid, hzoauthsecret):
    # TODO : Funtion call in azureMMMessageprocessor.called by asyncio loop.
    try:
        # Function to retrive infra details.
        azure_mgmt_url = 'https://management.azure.com/'
        token = acquire_authtoken(
            tenantid, clientid, clientsecret, azure_mgmt_url)
        headers = form_auth_headers(token)
        # Function to fetch ssubscription, resource group, resources,usage
        async with aiohttp.ClientSession(headers=headers, trust_env=True) \
                as client:
            if tenantmon_flag == "True" or tenantmon_flag == "true":
                print("Tenant Monitoring configured")
                azure_infra = AzureInfra(tenantname,tenantid, clientid, clientsecret,productType,customerId,citrixId,citrixSecret,hzurl, hzoauthid, hzoauthsecret)
            else:
                azure_infra = AzureInfra(tenantname, tenantid, clientid, clientsecret, productType, customerId, citrixId, citrixSecret, hzurl, hzoauthid, hzoauthsecret, subid)
            await azure_infra.fetch(client, headers, azure_mgmt_url)
            # TODO: Remove this code. Written for testing XML generation.
            '''
            for subscr in azure_infra.subscriptions:
                subxml = subscr.to_xml()
                with open('sample_output.xml', 'w') as f:
                    xmlstr = xml.tostring(subxml, pretty_print=True)
                    xmlstr = xmlstr.decode("utf-8")
                    print(xmlstr, file=f)
            '''
            return azure_infra
    except Exception as e:
        atlogger.error("Exception occured in discovery "
                       "of infra and wvd details -> {}".format(e))
        traceback.print_exception(*sys.exc_info())
        return None 

class CitrixDetails:

    def __init__(self, subscription_id, display_name, creds):
        atlogger.debug("Entering into CitrixDetails init function")       
        atlogger.debug("SUBSCRIPTION LIST:%s %sDISPLAY NAME:%s %s",subscription_id,display_name,creds.__dict__,self.__dict__)
        self.hypervisors=None
        self.machines=None
        self.desktopgroups=None
        self.catalogs=None
        self.customerId = creds.customerId
        atlogger.debug("CUSTOMER ID:%s",self.customerId)
        self.productType=creds.productType
        #Citrix Control Plane
        #self.clientid = creds.applicationid
        self.citrixId= creds.citrixId
        self.clientSecert= creds.citrixSecret
        #self.clientsecret = creds.appsecret
        self.display_name = display_name
        self.subscription_id = subscription_id

    async def getCitrixDetails(self, requestUrl, headers1):
        atlogger.debug("Entering to fetch pagination details")
        skip=0
        getResponse =requests.get(requestUrl, headers=headers1,params="$skip={}".format(skip)).json()
        if('@odata.nextLink' in getResponse):
            getAllResponse = []
            # skip=0
            while '@odata.nextLink' in getResponse:
                getResponse =requests.get(requestUrl, headers=headers1,params="$skip={}".format(skip)).json()
                getAllResponse.extend(getResponse['value'])
                skip = skip + 100
                #atlogger.debug("getResponse code %s",getResponse)
            return getAllResponse
        else:
            return getResponse['value']
   
    async def citrixHypervisors(self, headers1):
        atlogger.debug("Entering to fetch hypervisor details")
        lifecycleState="?$apply=filter(LifecycleState eq 0)"
        hypervisorsUrl = "https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/Hypervisors{}".format(self.customerId,lifecycleState)
        getAllResponse = await self.getCitrixDetails(hypervisorsUrl, headers1)
        return getAllResponse

    async def citrixMachines(self, headers1):
       atlogger.debug("Entering to fetch Machines details")
       lifecycleState="?$apply=filter(LifecycleState eq 0)"
       machinesUrl = "https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/Machines{}".format(self.customerId,lifecycleState)
       getAllResponse = await self.getCitrixDetails(machinesUrl, headers1)
       #atlogger.debug("getAllResponse %s",getAllResponse)
       return getAllResponse

    async def citrixCatalogs(self, headers1):
        atlogger.debug("Entering to fetch catalogs details")
        lifecycleState="?$apply=filter(LifecycleState eq 0)"
        catalogsUrl = "https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/catalogs".format(self.customerId,lifecycleState)
        getAllResponse = await self.getCitrixDetails(catalogsUrl, headers1)
        return getAllResponse

    async def citrixDesktopgroups(self, headers1):
        atlogger.debug("Entering to fetch Desktopgroups details")
        lifecycleState="?$apply=filter(LifecycleState eq 0)"
        desktopgroupsUrl = "https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/desktopgroups".format(self.customerId,lifecycleState)
        getAllResponse = await self.getCitrixDetails(desktopgroupsUrl, headers1)
        return getAllResponse

    async def citrixUserProvision(self,headers1):
        dateFiltering=datetime.now().strftime("%Y-%m-%d")
        dateFilterUrl="?$apply=filter(StartDate ge "+dateFiltering+"T00:00:00.0Z)"
        sessionsurl="https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/sessions".format(self.customerId)
        sessionsurl= sessionsurl + dateFilterUrl
        #print(sessionsurl)
        getAllResponse = await self.getCitrixDetails(sessionsurl, headers1)
        return getAllResponse

    async def citrixUserDetails(self,headers1):
        usersUrl = "https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/Users".format(self.customerId)
        getAllResponse = await self.getCitrixDetails(usersUrl,headers1)
        return getAllResponse

    async def citrixClusterDetails(self,headers1):
        clusterUrl = "https://api-ap-s.cloud.com/cvad/manage/hypervisors"
        getclusterResponse =requests.get(clusterUrl, headers=headers1)
        clusterData=getclusterResponse.json()
        clusterIps= []
        for clusterip in clusterData['Items']:
            clusterIps.append(clusterip["Addresses"][0])
        return clusterData,clusterIps


    async def acropolisHypervisor(self,clusterIps):
        atlogger.debug("Entering into acropolisHypervisori %s",clusterIps)
        try:
            conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='postgres', database='anuntatech')
            cur = conn.cursor()
            for clus in clusterIps: 
                #device_count = session.query(HostpoolDetails).filter_by(infosrcip = clus).all()
                #atlogger.debug("HostpoolDetails fetch1221 %s",device_count)
                #device_countqry = "select count(*) from hostpooldetails where infosrcip = '{}'".format(clus)
                #device_count = cur.execute(device_countqry)
                #device_count = cur.fetchall()
                device_countqry = "SELECT username, count(*) FROM hostpooldetails where infosrcip = '{}' GROUP BY username".format(clus)
                atlogger.debug(device_countqry)
                device_count = cur.execute(device_countqry)
                device_count = cur.fetchall()
                atlogger.debug(device_count)
                #if len(device_count) > 0:
                devicecount = device_count[0][1]
                if devicecount != 0:
                    #username_qry = "select username from hostpooldetails where infosrcip = '{}'".format(clus)
                    #users = session.query(HostpoolDetails).with_entities(HostpoolDetails.username).filter_by(infosrcip = clus).all()
                    #atlogger.debug("HostpoolDetails fetch11 %s",users)
                    #atlogger.debug("HostpoolDetails fetch1331 %s",users.username)
                    #username = cur.execute(username_qry)
                    #username = cur.fetchall()
                    user = device_count[0][0]
                    Dec_passwordqry = "select distinct pgp_sym_decrypt(password::bytea, '-----BEGIN PGP PUBLIC KEY BLOCK-----Version: GnuPG v1mI0EVjGkQAEEAKQ4W/gCAM5pnrBHxNUCGA4vhkbfHMpP4xWHPhbpA9PechyZmtI+gphWhNgtNdWrjgmri+i8EDIhcWTb4HBzLy1BRnjNxhHNP5VM/uacz9wDeSE/fgIhGGCbvu//ywB1QZmDsAmhC1JGaEfj+GgWsYHaaR0Eo25uwRJ5yBJqix3xABEBAAG0KUFudW50YXRlY2ggKEdTTEFCKSA8YW51bnRhdGVjaEBnc2xhYi5jb20+iLgEEwECACIFAlYxpEACGwMGCwkIBwMCBhUIAgkKCwQWAgMBAh4BAheAAAoJEAJitGNhuC9gTvQD/RMeAA6QGqyLxGhvlxCTT/udDANOP1XlvXTO2y1NTLPDSHT4+c0/85rLdcY4IjtmiWGOx8ISPvBzbsX73oI6LNlZ9HLEtR80Gz/yEGVQ9TgRcEAZI3RzMHvGoPHWLdVn+ce2tA3bjD6qnJG1NPjpU4xmFssquG4z1cg9NJ28Z3IduI0EVjGkQAEEAOOmwrdEiHUXfv8U9PrKulyoqo3zicS8NWO+PrU5IBJ5i8TbGM/gBecc21LkHXoQjCQhDk9ngLHLiqvgsOrh2yonGhdB0goR+wKtOcswbp9ehqwxUI3FsOCfa9Zju3/uMxc1j7STbpEas3E0bLBU0Vw9ynDplNQPBapdLj15IWwLABEBAAGInwQYAQIACQUCVjGkQAIbDAAKCRACYrRjYbgvYOdXA/0Wm6sH0T6DzIDgFspWliw7dv+/4aZWByg20lQyJll38Ze69tYRf2PZo7IZBMxIQHXkveRXUO8xQ1W+S10uukzQWDO6zyyNkdDxPDfH2gHygJztg/gfXwNA9XnPP1ZFPduLwilhNdm/eZFJXtwCdwSvGVSxOYXjhwsz0RH4wZLYfg===9hIy-----END PGP PUBLIC KEY BLOCK-----')from hostpooldetails where infosrcip = '{}'".format(clus)
                    Dec_password = cur.execute(Dec_passwordqry)
                    Dec_password = cur.fetchall()
                    password = Dec_password[0][0]
                    url = 'https://%s:9440/PrismGateway/services/rest/v2.0/hosts/'%(clus)
                    result = requests.get(url,auth=(user,password),verify = False).json()
                    atlogger.debug("error code %s",result.status_code)
                    if result.status_code == 200:
                        acropolishyp = []
                        for i in result['entities']:
                            devicedict = {}
                            devicedict['acropolisHypIp'] = i['management_server_name']
                            devicedict['hostname'] = i['name']
                            devicedict['IpAddress'] = clus
                        acropolishyp.append(devicedict)
                        atlogger.debug("acropolisHypervisor Hostname %s",devicedict)
                        return True,acropolishyp

                    elif result.status_code == 429:
                        atlogger.debug("Entered sllep api call %s",result.status_code)
                        await asyncio.sleep(60 * 5)
                else:
                    atlogger.debug("No entries present in hostpooldetails tables for")
                cur.close()
                conn.close()
                
        except Exception as e:
            print ("Error in hostname function...", e)
            return False,e


    async def citrix_fetch(self):
        try:
            atlogger.debug("calling citrix fetch function %s",self.__dict__)
            url = "https://trust.citrixworkspacesapi.net/root/tokens/clients"
            headers = {"Content-Type": "application/json"}
            #data = {"ClientId":"13a63934-fc00-4624-bdf9-3666a8bf93f9","ClientSecret": "0vxlFWdDSOK3cCflb7KfFw=="}
            data = {"ClientId":self.citrixId,"ClientSecret":self.clientSecert} 
            response = requests.post(url, headers=headers, json=data)
            #atlogger.debug("citrix auth token raw response %s and status code is %s",response,response.status_code)
            Bearertoken= response.json()
            token = "CwsAuth Bearer="+str(Bearertoken['token'])
            headers1 = {"Authorization": token,"Customer":self.customerId}
            atlogger.debug("citrix bearer token :%s",headers1)
            
            self.hypervisors =  await self.citrixHypervisors(headers1)
            #atlogger.debug("Citrix Hypervisors Lists :%s" ,self.hypervisors)
            
            self.machines =  await self.citrixMachines(headers1)
            #atlogger.debug("Citrix Machines Lists :%s" ,self.machines)
            
            self.catalogs =  await self.citrixCatalogs(headers1)
            #atlogger.debug("Citrix Catalogs Lists :%s" ,self.catalogs)
            #with open("/home/anunta/catalogs.json","w") as f:
            #    f.write(json.dumps(self.catalogs,indent=4))

            self.desktopgroups =  await self.citrixDesktopgroups(headers1)
            #atlogger.debug("Desktopgroups Lists :%s" ,self.desktopgroups)
            #with open("/home/anunta/desktopgroups.json","w") as f:
            #    f.write(json.dumps(self.desktopgroups,indent=4))

            self.UserProvisions = await self.citrixUserProvision(headers1)
            #atlogger.debug("self.UserProvisions %s",self.UserProvisions)

            self.usersDetails = await self.citrixUserDetails(headers1)
            #atlogger.debug("self.usersDetails %s",self.usersDetails)
            if self.productType!='Citrix Control Plane':
                try: 
                    authtoken ={"Authorization": token,"Citrix-CustomerId":self.customerId}
                    instanceUrl="https://api-ap-s.cloud.com/catalogservice/{}/sites".format(self.customerId)
                    getInstanceResponse =requests.get(instanceUrl, headers=authtoken)
                    if getInstanceResponse.status_code == 429:
                        atlogger.debug("Entered sllep api call %s",getInstanceResponse.status_code)
                        await asyncio.sleep(60 * 5)
    
                    instanceData=getInstanceResponse.json()
                    if getInstanceResponse.status_code==200:
                        instanceId=instanceData['items'][0]['id']
                        authtoken["Citrix-InstanceId"]=str(instanceId)
                    else:
                        atlogger.debug("failed to fetch :",instanceUrl)
                    self.clustersDetails,self.clusterIps = await self.citrixClusterDetails(authtoken)
                    atlogger.debug("self.clustersDetails %s",self.clustersDetails)
                    if self.clusterIps != None:
                        self.flag,self.acropolisHypervisors = await self.acropolisHypervisor(self.clusterIps)
                except Exception as e:
                    atlogger.debug("Fetch acropolisHypervisors for failed :%s",str(e))
                    traceback.print_exception(*sys.exc_info())
                    return None
        except Exception as e:
            atlogger.debug("Fetch failed :%s",str(e))
            traceback.print_exception(*sys.exc_info())
            return {}
     
    def to_xml(self,param):

        #atlogger.debug("citrix response,citrix params %s",param)
        citrixxml = xml.Element('citrixDetails')
        if param==2:
            atlogger.debug("Entering to generate citrix response")
            # Adding Hypervisors,Machines,catalogs,DesktopGroups  details
            hypxml = xml.Element('hypervisorDetails')
            #atlogger.debug("tenxml hypervisorDetails %s",hypxml) 
            if self.display_name != None:
                hypName=self.display_name
                #atlogger.debug("hypervisor display name %s", type(self.display_name))
                splitName=hypName.split("-")[-1]
                atlogger.debug("hypervisor split name %s",splitName)
            #atlogger.debug(type(splitName))

            hypid =''
            atlogger.debug("Hypervisors List:%s",self.hypervisors)
            #dsname=0
            for hypList in self.hypervisors:
                atlogger.debug("in for loop for hypervisors")
                atlogger.debug(type(hypList))
                #if splitName in hypList["Name"]:
                #atlogger.debug("if condition split  name in xml :%s, %s",hypList["Name"], splitName)
                atlogger.debug("%s %s",type(hypList["Name"]),hypList.get('Name'))
                hypid = hypList["Id"]
                atlogger.debug("hypervisors id in xml :%s,%s,%s",hypid, hypList["Id"],hypList.get('Id'))
                atlogger.debug(type(hypList["Id"]),hypList.get('Id'))
                hypxml.append(xml_tag('hypervisorName', splitName))
                hypxml.append(xml_tag('hypervisorId', hypList.get('Id')))
                if hypList["LifecycleState"]==0:
                    lstate="Active"
                elif  hypList["LifecycleState"]==1:
                    lstate ="Deleted"
                elif hypList["LifecycleState"]==2:
                    lstate = "Requires Resolution"
                elif hypList["LifecycleState"]==3:
                    lstate = "Stub"
                atlogger.debug("hypervisors LifecycleState in xml :%s",hypList["LifecycleState"])
                atlogger.debug(type(hypList["LifecycleState"]))
                hypxml.append(xml_tag('lifeCycleState', lstate))
                atlogger.debug("azureSubscriptionId in xml :%s",self.subscription_id)
                atlogger.debug(type(self.subscription_id))
                hypxml.append(xml_tag('azureSubscriptionId', self.subscription_id))
            machineinfoxml = xml.Element('machineInfo')
            #mxml = xml.Element('machine')
            cataloglist = []
            desktoplist = []
            for machineList in self.machines:
                atlogger.debug("in for loop in machines")
                machinexml = xml.Element('machine')
                if hypid == machineList['HypervisorId'] :
                    machinexml.append(xml_tag('machineName', machineList.get('Name')))
                    machinexml.append(xml_tag('currentSessionsCount', machineList.get('CurrentSessionCount')))
                    if  machineList['CurrentPowerState'] == 3:
                        state = 'UP'
                    elif  machineList['CurrentPowerState'] == 2  or  machineList['CurrentPowerState'] == 6:
                        state = 'DOWN'
                    else :
                        state = ''
                    machinexml.append(xml_tag('currentPowerState', state))
                    #if machineList['CurrentRegistrationState'] == 2:
                    #    regstate = 'UnRegistered'
                    if  machineList['CurrentRegistrationState'] ==1:
                        regstate = 'Registered'
                    else:
                        regstate = 'UnRegistered'
                    machinexml.append(xml_tag('registrationState', regstate))
                    machinexml.append(xml_tag('currentPoweredOnDate', machineList.get('PoweredOnDate')))
                    #if machineList['IPAddress'] != None:
                    machinexml.append(xml_tag('ipAddress', machineList.get('IPAddress')))
                    machinexml.append(xml_tag('agentVersion', machineList.get('AgentVersion')))
                    machinexml.append(xml_tag('osType', machineList.get('OSType')))
                    machinexml.append(xml_tag('controllerDnsName', machineList.get('ControllerDnsName')))
                    machinexml.append(xml_tag('dnsName', machineList.get('DnsName')))
                    if machineList['CatalogId'] not in cataloglist and machineList['CatalogId'] != None:
                        cataloglist.append(machineList['CatalogId'])
                    if machineList['DesktopGroupId'] not in desktoplist and machineList['DesktopGroupId'] != None:
                        desktoplist.append( machineList['DesktopGroupId'])
                    machinexml.append(xml_tag('catalogId', machineList.get('CatalogId')))
                    machinexml.append(xml_tag('desktopGroupId', machineList.get('DesktopGroupId')))
                    machineinfoxml.append(machinexml)

            hypxml.append(machineinfoxml)
            cataloginfoxml = xml.Element('catalogInfo')
            atlogger.debug("Finished Fethed MachinesList")

            for catalogList in self.catalogs:
                atlogger.debug("in for loop catalogs")
                catalogxml = xml.Element('catalog')
                if catalogList['Id'] in cataloglist:
                    catalogxml.append(xml_tag('id', catalogList['Id']))
                    catalogxml.append(xml_tag('name', catalogList['Name']))
                    sessiontype = 'Single' if catalogList['SessionSupport'] ==1 else 'Multi'
                    catalogxml.append(xml_tag('sessionType', sessiontype))
                    if catalogList['IsMachinePhysical'] == True or catalogList['IsMachinePhysical'] == 'True' or catalogList['IsMachinePhysical'] == 'true':
                        machinetype ='Physical'
                    else:
                        machinetype = 'Virtual'
                    catalogxml.append(xml_tag('machineType', machinetype))
                    #Unknown Static Random Permanent
                    if catalogList['AllocationType'] ==0:
                        allocationtype = 'Unknown'
                    elif catalogList['AllocationType'] ==1:
                        allocationtype ='Static'
                    elif catalogList['AllocationType'] ==2:
                        allocationtype ='Random'
                    else:
                        allocationtype='Permanent'
                    catalogxml.append(xml_tag('allocationType', allocationtype))
                    if catalogList['ProvisioningType'] ==0:
                        provisiontype ='Unknown'
                    elif catalogList['ProvisioningType'] ==1:
                        provisiontype ='MCS'
                    elif catalogList['ProvisioningType'] ==2:
                        provisiontype ='PVS'
                    else:
                        sessiontype='Manual'
                    catalogxml.append(xml_tag('provisioningType', provisiontype))
                    cataloginfoxml.append(catalogxml)
            hypxml.append(cataloginfoxml)

            atlogger.debug("Finished Fetched Catalog")

            desktopinfoxml = xml.Element('desktopGroupInfo')
            #dxml =xml.Element('desktopGroup')
            atlogger.debug("Before for in fetch desktop")
            for desktopList in self.desktopgroups:
                atlogger.debug("in for deskopgroups")
                desktopgroupxml =xml.Element('desktopGroup')
                if desktopList['Id'] in desktoplist:
                    desktopgroupxml.append(xml_tag('id', desktopList.get('Id')))
                    desktopgroupxml.append(xml_tag('name', desktopList.get('Name')))
                    #DesktopsOnly  AppsOnly   DesktopsAndApps
                    if desktopList['DeliveryType'] ==0:
                        desktopgrouptype ='DesktopsOnly'
                    if desktopList['DeliveryType'] ==1:
                        desktopgrouptype ='AppsOnly'
                    if desktopList['DeliveryType'] ==2:
                        desktopgrouptype='DesktopsAndApps'
                    desktopgroupxml.append(xml_tag('deliveryType',desktopgrouptype))
                    desktopKind="Private" if desktopList['DesktopKind'] ==0 else "Shared"
                    desktopgroupxml.append(xml_tag('desktopKind', desktopKind))
                    sessionsupport ="Single" if desktopList['SessionSupport']==1 else "Multi"
                    desktopgroupxml.append(xml_tag('sessionSupport',sessionsupport))
                    desktopinfoxml.append(desktopgroupxml)
            hypxml.append(desktopinfoxml)

            citrixxml.append(hypxml)
            atlogger.debug("xml finished")
            return citrixxml
        if param ==3:
            citrixxml = xml.Element('citrixDetails')
            userprovisionxml= xml.Element('usersProvisioned')    
            #if self.productType!='Citrix Control Plane': 
            #hypName=self.display_name # uncomment for ccoa product
            #splitName=hypName.split("-")[-1] # uncomment for ccoa product

            hypervisorIdList=[]
            for hypList in self.hypervisors:
                #if splitName in hypList["Name"]: # uncomment for ccoa product
                #atlogger.debug("fetching hypervisors name inside user provisioning xml : %s ", splitName)
                hypervisorIdList.append(hypList["Id"])
            atlogger.debug("fetching hypervisors id in a list inside user provisioning xml : %s ",  hypervisorIdList)
            
            machineIdList=[]
            for user in self.UserProvisions:
                if user['MachineId']!=None:
                     machineIdList.append(user['MachineId'])
            atlogger.debug("fetching machine id in a list from  sessions, inside user provisioning xml : %s ",  machineIdList)
            
            for machines in self.machines:
                #if machines['Id'] in  machineIdList:
                if machines['AssociatedUserUPNs'] != None and machines['AssociatedUserUPNs'] !='' :
                    userxml = xml.Element('user')
                    if ',' in machines.get('AssociatedUserUPNs'):
                        ulist=machines.get('AssociatedUserUPNs')
                        userslist=ulist.split(',')
                        #for ul in userslist:
                        userxml.append(xml_tag('userPrincipalName',userslist[0]))
                    else:
                        userxml.append(xml_tag('userPrincipalName',machines.get('AssociatedUserUPNs')))
                    atlogger.debug("fetching AssociatedUserUPNs  from machines, inside user provisioning xml : %s ", machines['AssociatedUserUPNs'])
                    for desktops in self.desktopgroups:
                        if machines['DesktopGroupId'] == desktops['Id']:
                            desktopKind="Private" if desktops['DesktopKind'] ==0 else "Shared"
                            userxml.append(xml_tag('desktopType',desktopKind))
                            userxml.append(xml_tag('desktopGroupId',desktops.get('Id')))
                            atlogger.debug("fetching desktoptype and id in a list  from machines, inside user provisioning xml : %s %s",desktopKind,desktops['Id'])
                        #if machines['HypervisorId'] in  hypervisorIdList:
                    if self.productType!='Citrix Control Plane':
                        if  machines.get('HypervisorId') != None and machines.get('HypervisorId') != '':
                            userxml.append(xml_tag('hypervisorId', machines.get('HypervisorId')))
                        else:
                            userxml.append(xml_tag('hypervisorId',''))
                    atlogger.debug("fetching HypervisorId  from machines, inside user provisioning xml : %s ", machines['HypervisorId'])
                    userprovisionxml.append(userxml)
            #atlogger.debug("USER PROVISIONED XML:%s",userprovisionxml)
            citrixxml.append(userprovisionxml)
            return citrixxml

    def cconcitrixDetailsxml(self,param):
        if param==2:
            citrixxml = xml.Element('citrixDetails')
            atlogger.debug("Going to call citrixDetails")
            for clustersss in self.clustersDetails['Items']:
                for hyp in self.hypervisors:
                    if clustersss['Id'] == hyp['Id']:
                        atlogger.debug("Going to call citrixDetails %s",clustersss)
                        cluster=xml.Element("clusterInfo")
                        cluster.append(xml_tag('ipAddress',str(clustersss["Addresses"][0])))
                        cluster.append(xml_tag('clusterId',str(clustersss["Id"])))
                        cluster.append(xml_tag('clustername',str(clustersss["Name"])))
                        cluster.append(xml_tag('XDPath',str(clustersss["XDPath"])))
                        if hyp["LifecycleState"]==0:
                            lstate="Active"
                        elif  hyp["LifecycleState"]==1:
                            lstate ="Deleted"
                        elif hyp["LifecycleState"]==2:
                            lstate = "Requires Resolution"
                        elif hyp["LifecycleState"]==3:
                            lstate = "Stub"
                        cluster.append(xml_tag('lifeCycleState',lstate))


                        machineinfoxml = xml.Element('machineInfo')
                        cataloglist = []
                        desktoplist = []
                        for machineList in self.machines:
                            if machineList['HypervisorId'] == hyp.get('Id'):
                                machinexml = xml.Element('machine')
                                machinexml.append(xml_tag('machineName', machineList.get('Name')))
                                machinexml.append(xml_tag('currentSessionsCount', machineList.get('CurrentSessionCount')))
                                if machineList['CurrentPowerState'] == 3:
                                    state = 'UP'
                                elif  machineList['CurrentPowerState'] == 2  or  machineList['CurrentPowerState'] == 6:
                                    state = 'DOWN'
                                else :
                                    state = ''
                                machinexml.append(xml_tag('currentPowerState', state))
                                if  machineList['CurrentRegistrationState'] ==1:
                                    regstate = 'Registered'
                                else:
                                    regstate = 'UnRegistered'
                                machinexml.append(xml_tag('registrationState', regstate))
                                machinexml.append(xml_tag('currentPoweredOnDate', machineList.get('PoweredOnDate')))
                                machinexml.append(xml_tag('ipAddress', machineList.get('IPAddress')))
                                machinexml.append(xml_tag('agentVersion', machineList.get('AgentVersion')))
                                machinexml.append(xml_tag('osType', machineList.get('OSType')))
                                machinexml.append(xml_tag('controllerDnsName', machineList.get('ControllerDnsName')))
                                machinexml.append(xml_tag('dnsName', machineList.get('DnsName')))
                                if machineList['CatalogId'] not in cataloglist and machineList['CatalogId'] != None:
                                    cataloglist.append(machineList['CatalogId'])
                                if machineList['DesktopGroupId'] not in desktoplist and machineList['DesktopGroupId'] != None:
                                    desktoplist.append( machineList['DesktopGroupId'])
                                machinexml.append(xml_tag('catalogId', machineList.get('CatalogId')))
                                machinexml.append(xml_tag('desktopGroupId', machineList.get('DesktopGroupId')))

                                machineinfoxml.append(machinexml)

                        cluster.append(machineinfoxml)

                        cataloginfoxml = xml.Element('catalogInfo')
                        atlogger.debug("Going to call catalogs")
                        for catalogList in self.catalogs:
                            catalogxml = xml.Element('catalog')
                            if catalogList['Id'] in cataloglist:
                                catalogxml.append(xml_tag('id', catalogList['Id']))
                                catalogxml.append(xml_tag('name', catalogList['Name']))
                                sessiontype = 'Single' if catalogList['SessionSupport'] ==1 else 'Multi'
                                catalogxml.append(xml_tag('sessionType', sessiontype)) 
                                if catalogList['IsMachinePhysical'] == True or catalogList['IsMachinePhysical'] == 'True' or catalogList['IsMachinePhysical'] == 'true':
                                    machinetype ='Physical'
                                else:
                                    machinetype = 'Virtual'
                                catalogxml.append(xml_tag('machineType', machinetype))
                                if catalogList['AllocationType'] ==0:
                                    allocationtype = 'Unknown'
                                elif catalogList['AllocationType'] ==1:
                                    allocationtype ='Static'
                                elif catalogList['AllocationType'] ==2:
                                    allocationtype ='Random'
                                else:
                                    allocationtype='Permanent'
                                catalogxml.append(xml_tag('allocationType', allocationtype))
                                if catalogList['ProvisioningType'] ==0:
                                    provisiontype ='Unknown'
                                elif catalogList['ProvisioningType'] ==1:
                                    provisiontype ='MCS'
                                elif catalogList['ProvisioningType'] ==2:
                                    provisiontype ='PVS'
                                else:
                                    provisiontype='Manual'
                                catalogxml.append(xml_tag('provisioningType', provisiontype))
                                cataloginfoxml.append(catalogxml)
                        cluster.append(cataloginfoxml)
                        atlogger.debug("Finished Fetched catalogInfo")

                        desktopinfoxml = xml.Element('desktopGroupInfo')
                        atlogger.debug("Going to call  desktop")
                        for desktopList in self.desktopgroups:
                            desktopgroupxml =xml.Element('desktopGroup')
                            if desktopList['Id'] in desktoplist:
                                desktopgroupxml.append(xml_tag('id', desktopList.get('Id')))
                                desktopgroupxml.append(xml_tag('name', desktopList.get('Name')))
                                if desktopList['DeliveryType'] ==0:
                                    desktopgrouptype ='DesktopsOnly'
                                if desktopList['DeliveryType'] ==1:
                                    desktopgrouptype ='AppsOnly'
                                if desktopList['DeliveryType'] ==2:
                                    desktopgrouptype='DesktopsAndApps'
                                desktopgroupxml.append(xml_tag('deliveryType',desktopgrouptype))
                                desktopKind="Private" if desktopList['DesktopKind'] ==0 else "Shared"
                                desktopgroupxml.append(xml_tag('desktopKind', desktopKind))
                                sessionsupport ="Single" if desktopList['SessionSupport']==1 else "Multi"
                                desktopgroupxml.append(xml_tag('sessionSupport',sessionsupport))
                                desktopinfoxml.append(desktopgroupxml)
                        cluster.append(desktopinfoxml)

                        HypervisorInfo = xml.Element('acropolisHypervisorInfo')
                        if self.flag ==True:
                            for acropolis in self.acropolisHypervisors:
                                if hyp['Id'] == clustersss['Id'] and acropolis.get('hostname') != None and acropolis.get('acropolisHypIp') != None:
                                    HypervisorInfoxml=xml.Element('acropolisHypervisor')
                                    HypervisorInfoxml.append(xml_tag('hostname',acropolis.get('hostname')))
                                    HypervisorInfoxml.append(xml_tag('ipAddress',acropolis.get('acropolisHypIp')))
                                    HypervisorInfoxml.append(xml_tag('hypervisorId',acropolis.get(clustersss["Id"])))
                                    HypervisorInfoxml.append(xml_tag('lifeCycleState',acropolis.get(lstate)))

                                    HypervisorInfo.append(HypervisorInfoxml)
                                #else:
                                #    HypervisorInfo.text= None
                        cluster.append(HypervisorInfo)

                        citrixxml.append(cluster)
            return citrixxml

        if param == 3:
            atlogger.debug("Entering into usersProvisioned for Nutanix")
            usersProvisioned = xml.Element('citrixDetails')
            userprovisionxml= xml.Element('usersProvisioned')
            #hypervisorIdList=[hypList["Id"] for hypList in self.hypervisors ]
            #machineIdList=[user['MachineId'] for user in self.UserProvisions if user['MachineId']!=None]
            for machines in self.machines:
                if machines['AssociatedUserUPNs'] != None and machines['AssociatedUserUPNs'] !='' :
                    '''userxml = xml.Element('user')
                    userxml.append(xml_tag('userPrincipalName',machines.get('AssociatedUserUPNs')))
                    for desktops in self.desktopgroups:
                        if machines['DesktopGroupId'] == desktops['Id']:
                            desktopKind="Private" if desktops['DesktopKind'] ==0 else "Shared"
                            userxml.append(xml_tag('desktopType',desktopKind))
                            userxml.append(xml_tag('desktopGroupId',desktops.get('Id')))
                    if  machines.get('HypervisorId') != None and machines.get('HypervisorId') != '':
                        userxml.append(xml_tag('clusterId', machines.get('HypervisorId')))
                    else:
                        userxml.append(xml_tag('clusterId',''))'''
            
                    if "," in machines['AssociatedUserUPNs']:
                        splitName=machines['AssociatedUserUPNs'].split(",")
                        countNames=len(splitName) 
                        if countNames >1:
                            for usersItemCount in range(countNames):
                                userxml = xml.Element('user')
                                userxml.append(xml_tag('userPrincipalName',str(splitName[usersItemCount])))
                                for desktops in self.desktopgroups:
                                    if machines['DesktopGroupId'] == desktops['Id']:
                                        desktopKind="Private" if desktops['DesktopKind'] ==0 else "Shared"
                                        userxml.append(xml_tag('desktopType',desktopKind))
                                        userxml.append(xml_tag('desktopGroupId',desktops.get('Id')))
                                if  machines.get('HypervisorId') != None and machines.get('HypervisorId') != '':
                                    userxml.append(xml_tag('clusterId', machines.get('HypervisorId')))
                                else:
                                    userxml.append(xml_tag('clusterId',''))
                    else:
                        userxml = xml.Element('user')
                        userxml.append(xml_tag('userPrincipalName',machines.get('AssociatedUserUPNs')))
                        for desktops in self.desktopgroups:
                            if machines['DesktopGroupId'] == desktops['Id']:
                                desktopKind="Private" if desktops['DesktopKind'] ==0 else "Shared"
                                userxml.append(xml_tag('desktopType',desktopKind))
                                userxml.append(xml_tag('desktopGroupId',desktops.get('Id')))
                        if  machines.get('HypervisorId') != None and machines.get('HypervisorId') != '':
                            userxml.append(xml_tag('clusterId', machines.get('HypervisorId')))
                        else:
                            userxml.append(xml_tag('clusterId',''))
                    userprovisionxml.append(userxml)
            usersProvisioned.append(userprovisionxml)
            atlogger.debug("Finished Fetched usersProvisioned for Nutanix")
            return usersProvisioned

        if param == 4:
            atlogger.debug("Entering into total users for Nutanix")
            usersdta = xml.Element('citrixDetails')
            users = xml.Element('users')
            for usr in self.usersDetails:
                userxml = xml.Element('user')
                if usr['Upn'] == '' :
                    userName = usr['UserName']
                else:
                    userName = usr['Upn']
                userxml.append(xml_tag('userPrincipalName',userName))
                userxml.append(xml_tag('userId',usr['Sid']))
                users.append(userxml)
            usersdta.append(users)
            return usersdta
    def citrixControlPlane_xml(self,param):
        if param==1:
            citrixxml = xml.Element('citrixDetails')
            hypInfoxml = xml.Element('hypervisorDetail')
            for hypList in self.hypervisors:
                hypxml = xml.Element('hypervisor')
                hypxml.append(xml_tag('hypervisorName', hypList["Name"]))
                hypxml.append(xml_tag('hypervisorId', hypList.get('Id')))
                if hypList["LifecycleState"]==0:
                    lstate="Active"
                elif  hypList["LifecycleState"]==1:
                    lstate ="Deleted"
                elif hypList["LifecycleState"]==2:
                    lstate = "Requires Resolution"
                elif hypList["LifecycleState"]==3:
                    lstate = "Stub"
                atlogger.debug("hypervisors LifecycleState in xml :%s",hypList["LifecycleState"])
                atlogger.debug(type(hypList["LifecycleState"]))
                hypxml.append(xml_tag('lifeCycleState', lstate))
                hypInfoxml.append(hypxml)
            citrixxml.append(hypInfoxml)
            machineinfoxml = xml.Element('machineInfo')
            cataloglist = []
            desktoplist = []
            for machineList in self.machines:
                atlogger.debug("in for loop in machines")
                machinexml = xml.Element('machine')
                machinexml.append(xml_tag('machineName', machineList.get('Name')))
                machinexml.append(xml_tag('currentSessionsCount', machineList.get('CurrentSessionCount')))
                if  machineList['CurrentPowerState'] == 3:
                    state = 'UP'
                elif  machineList['CurrentPowerState'] == 2  or  machineList['CurrentPowerState'] == 6:
                    state = 'DOWN'
                else :
                    state= ''
                machinexml.append(xml_tag('currentPowerState', state))
                if  machineList['CurrentRegistrationState'] ==1:
                    regstate = 'Registered'
                else:
                    regstate = 'UnRegistered'
                machinexml.append(xml_tag('registrationState', regstate))
                machinexml.append(xml_tag('currentPoweredOnDate', machineList.get('PoweredOnDate')))
                #if machineList['IPAddress'] != None:
                machinexml.append(xml_tag('ipAddress', machineList.get('IPAddress')))
                machinexml.append(xml_tag('agentVersion', machineList.get('AgentVersion')))
                machinexml.append(xml_tag('osType', machineList.get('OSType')))
                machinexml.append(xml_tag('controllerDnsName', machineList.get('ControllerDnsName')))
                machinexml.append(xml_tag('dnsName', machineList.get('DnsName')))
                machinexml.append(xml_tag('catalogId', machineList.get('CatalogId')))
                machinexml.append(xml_tag('desktopGroupId', machineList.get('DesktopGroupId')))
                machineinfoxml.append(machinexml)
            #hypxml.append(machineinfoxml)
            #hypInfoxml.append(hypxml)
            citrixxml.append(machineinfoxml)
            cataloginfoxml = xml.Element('catalogInfo')
            for catalogList in self.catalogs:
                atlogger.debug("in for loop catalogs")
                catalogxml = xml.Element('catalog')
                catalogxml.append(xml_tag('id', catalogList['Id']))
                catalogxml.append(xml_tag('name', catalogList['Name']))
                sessiontype = 'Single' if catalogList['SessionSupport'] ==1 else 'Multi'
                catalogxml.append(xml_tag('sessionType', sessiontype))
                if catalogList['IsMachinePhysical'] == True or catalogList['IsMachinePhysical'] == 'True' or catalogList['IsMachinePhysical'] == 'true':
                    machinetype ='Physical'
                else:
                    machinetype = 'Virtual'
                catalogxml.append(xml_tag('machineType', machinetype))
                #Unknown Static Random Permanent
                if catalogList['AllocationType'] ==0:
                    allocationtype = 'Unknown'
                elif catalogList['AllocationType'] ==1:
                    allocationtype ='Static'
                elif catalogList['AllocationType'] ==2:
                    allocationtype ='Random'
                else:
                    allocationtype='Permanent'
                catalogxml.append(xml_tag('allocationType', allocationtype))
                if catalogList['ProvisioningType'] ==0:
                    provisiontype ='Unknown'
                elif catalogList['ProvisioningType'] ==1:
                    provisiontype ='MCS'
                elif catalogList['ProvisioningType'] ==2:
                    provisiontype ='PVS'
                else:
                    sessiontype='Manual'
                catalogxml.append(xml_tag('provisioningType', provisiontype))
                cataloginfoxml.append(catalogxml)
            citrixxml.append(cataloginfoxml)
            desktopinfoxml = xml.Element('desktopGroupInfo')
            atlogger.debug("Before for in fetch desktop")
            for desktopList in self.desktopgroups:
                atlogger.debug("in for deskopgroups")
                desktopgroupxml =xml.Element('desktopGroup')
                desktopgroupxml.append(xml_tag('id', desktopList.get('Id')))
                desktopgroupxml.append(xml_tag('name', desktopList.get('Name')))
                #DesktopsOnly  AppsOnly   DesktopsAndApps
                if desktopList['DeliveryType'] ==0:
                    desktopgrouptype ='DesktopsOnly'
                if desktopList['DeliveryType'] ==1:
                    desktopgrouptype ='AppsOnly'
                if desktopList['DeliveryType'] ==2:
                    desktopgrouptype='DesktopsAndApps'
                desktopgroupxml.append(xml_tag('deliveryType',desktopgrouptype))
                desktopKind="Private" if desktopList['DesktopKind'] ==0 else "Shared"
                desktopgroupxml.append(xml_tag('desktopKind', desktopKind))
                sessionsupport ="Single" if desktopList['SessionSupport']==1 else "Multi"
                desktopgroupxml.append(xml_tag('sessionSupport',sessionsupport))
                desktopinfoxml.append(desktopgroupxml)
            citrixxml.append(desktopinfoxml)
            return citrixxml
class CitrixSessionDetails:
    
    def citrix_session_fetch(self,message, reqId, mBus):
        atlogger.debug("Entering into CitrixSessionDetails %s",message)
        atlogger.debug("tenantid:%s clientId: %s clientSecret: %s ProductType: %s tenantName: %s",message['tenantId'],message['clientId'],message['clientSecret'],message['productType'], message['tenantName'])
        #atlogger.debug("MESSAGE ID:%s",message.tenantId)
        conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='postgres', database='anuntatech')
        curObj = conn.cursor()
        query = "select customerid, citrixid, citrixsecret from azureconfigdetails where tenantid ='{}'".format(message['tenantId'])
        #print("SECRETS ",citrixdetails[0][0],citrixdetails[0][1],citrixdetails[0][2])
        citrixdetails = curObj.execute(query)
        citrixdetails = curObj.fetchall()
        curObj.close()
        conn.close()
        print("SECRETS ",citrixdetails[0][0],citrixdetails[0][1],citrixdetails[0][2])
        url = "https://trust.citrixworkspacesapi.net/root/tokens/clients"
        headers = {"Content-Type": "application/json"}
        data = {"ClientId":citrixdetails[0][1],"ClientSecret": citrixdetails[0][2]}
        response = requests.post(url, headers=headers, json=data)
        Bearertoken= response.json()
        token = "CwsAuth Bearer="+str(Bearertoken['token'])
        headers1 = {"Authorization": token,"Customer":citrixdetails[0][0]}
        atlogger.debug("Auth token fetched for CitrixSessionDetails")

        def getDetails(requestUrl):
            skip=0
            getResponse =requests.get(requestUrl, headers=headers1,params="$skip={}".format(skip)).json()
            if('@odata.nextLink' in getResponse):
                getAllResponse = []
                # skip=0
                while '@odata.nextLink' in getResponse:
                    getResponse =requests.get(requestUrl, headers=headers1,params="$skip={}".format(skip)).json()
                    getAllResponse.extend(getResponse['value'])
                    skip = skip + 100
                return getAllResponse
            else:
                return getResponse['value']
        

        #Get all Hypervisors Info
        def hypervisors():
            print("Get all Hypervisors Inf")
            lifecycleState="?$apply=filter(LifecycleState eq 0)"
            hypervisorsUrl = "https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/Hypervisors{}".format(citrixdetails[0][0],lifecycleState)
            print(hypervisorsUrl)
            getAllResponse =  getDetails(hypervisorsUrl)
            return getAllResponse

        #Get all desktoproups Info
        def desktopgroups():
            lifecycleState="?$apply=filter(LifecycleState eq 0)"
            desktopgroupsUrl = "https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/desktopgroups{}".format(citrixdetails[0][0],lifecycleState)
            print(desktopgroupsUrl)
            getAllResponse =  getDetails(desktopgroupsUrl)
            return getAllResponse

        #Get all connections Info
        def connections():
            print("connections")
            #dateFiltering="2022-08-23"#datetime.now().strftime("%Y-%m-%d")
            Current_UTC = datetime.utcnow()
            ModifiedDate = (Current_UTC - timedelta(hours=12)).isoformat()
            ModifiedDate = ModifiedDate +"Z"
            dateFilterUrl="?$apply=filter(ModifiedDate ge "+ModifiedDate+")"
            connectionsUrl = "https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/connections{}".format(citrixdetails[0][0],dateFilterUrl)
            getAllResponse = getDetails(connectionsUrl)
            print(getAllResponse)
            return getAllResponse
 
        #Get all sessions Info
        def sessions():
            #dateFiltering="2022-08-23" #datetime.now().strftime("%Y-%m-%d")
            Current_UTC = datetime.utcnow()
            ModifiedDate = (Current_UTC - timedelta(hours=12)).isoformat()
            ModifiedDate = ModifiedDate +"Z"
            dateFilterUrl="?$apply=filter(ModifiedDate ge "+ModifiedDate+")"
            sessionsUrl = "https://{}.xendesktop.net/Citrix/Monitor/OData/v4/Data/sessions{}".format(citrixdetails[0][0],dateFilterUrl)
            getAllResponse = getDetails(sessionsUrl)
            return getAllResponse
        

        getHypervisorResponse = hypervisors()
        atlogger.debug("Successfully fetched Hypervisors for CitrixSessionDetails %s",getHypervisorResponse)

        getSessionResponse = sessions()
        atlogger.debug("Successfully fetched sessions Info %s",getSessionResponse)

        getDesktopgroupsResponse = desktopgroups()
        atlogger.debug("Successfully fetched desktoproups for CitrixSessionDetails %s",getDesktopgroupsResponse)

        getConnectionsResponse = connections()
        atlogger.debug("Successfully fetched connections Info for CitrixSessionDetails %s",getConnectionsResponse)

        conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='postgres', database='anuntatech')
        curObj = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        #Fetching "citrixclouduserdetails" details from table
        selquery="select id, username from citrixclouduserdetails"
        curObj.execute(selquery)
        usersList =curObj.fetchall()
        allUsersList = []
        for users in usersList:
            allUsersList.append(dict(users))
        atlogger.debug("fetched citrixclouduserdetails from table")

        #Fetching "citrixcloudmachinedetails" details from table
        selquery1="select  id, dnsname, hypervisorid, ipaddress, desktopgroupid from citrixcloudmachinedetails"
        curObj.execute(selquery1)
        machinesList =curObj.fetchall()
        getMachinesResponse = []
        for machines in machinesList:
            getMachinesResponse.append(dict(machines))
            for desktops in getDesktopgroupsResponse:
                if machines['desktopgroupid']==desktops['Id']:
                    selquery2 = "select hostpool,host from wvd_hostpools_n_hosts where hostpool ='{0}' and host = '{1}'".format(desktops['Name'],machines['dnsname'])
                    curObj.execute(selquery2)
                    checkdb = curObj.fetchall()
                    if len(checkdb) ==0:
                        selquery3="insert into wvd_hostpools_n_hosts (hostpool,host) values(%s,%s)"
                        dataquery = (desktops['Name'],machines['dnsname']) 
                        curObj.execute(selquery3,dataquery)
                        conn.commit()
        atlogger.debug("fetched citrixcloudmachinedetails from table %s",getMachinesResponse)

        def transfer_to_analytics(resoucePoolId):
            atlogger.debug("transfer_to_Analytics")
            vdi_tta_logs = open( "/var/log/upstart/transfer_To_Analytics_ctx.log", "a")
            subprocess.Popen(["bash", "-c","/root/anuntatech3/scripts/transfer_To_Analytics_tta_wvd.sh True {0}".format(resoucePoolId)],
                              stdout=vdi_tta_logs, stderr=vdi_tta_logs)
            vdi_tta_logs.close()


        def generateXmlResponse(subscription,startDate, endDate, out):
            session = ET.SubElement(subscription, "session")
            sessionServerName = ET.SubElement(session, "sessionServerName")
            sessionServerName.text = machineData['dnsname'] if machineData['dnsname']!=None or machineData['dnsname']!="" else ''

            sessionServerType = ET.SubElement(session, "sessionServerType")
            sessionServerType.text = None

            sessionServerIPAddress = ET.SubElement(session, "sessionServerIPAddress")
            sessionServerIPAddress.text = machineData['ipaddress'] if machineData['ipaddress']!=None else 'null'

            sessionId = ET.SubElement(session, "sessionId")
            sessionId.text = sessionsData['SessionKey']

            logOnTime = ET.SubElement(session, "logOnTime")
            logOnTime.text = sessionsData['StartDate']

            disconnectTime = ET.SubElement(session, "disconnectTime")
            disconnectTime.text = sessionsData['EndDate'] if sessionsData['EndDate'] !=None or sessionsData['EndDate'] !="" else 'null'

            state = ET.SubElement(session, "state")
            state.text = connectionState
                                
            userName = ET.SubElement(session, "userName")
            userName.text = usersData['username']

            desktopGroupId = ET.SubElement(session, "desktopGroupId")
            desktopGroupId.text = desktopgroupData['Id']

        def LoggedoffDelete(response_xml,mbus):
            print("Entering into LoggedoffDelete")
            root = ET.fromstring(response_xml)
            logged_off_server_ips = []
            active_server_ips = set()
            for users in root.findall('.//subscription/session'):
                state = users.find('state').text
                if state == 'Logged_Off':
                    server_ip = users.find('sessionServerIPAddress').text
                    logged_off_server_ips.append(server_ip)
                if state == 'Active' or state == 'Reconnected' or state == 'Disconnected':
                    server_ip = users.find('sessionServerIPAddress').text
                    active_server_ips.add(server_ip)
            print("logged_off_server_ips",logged_off_server_ips)
            print("active_server_ips",active_server_ips)
            common_server_ips = set(logged_off_server_ips).intersection(active_server_ips)
            logged_off_without_active = set(logged_off_server_ips) - common_server_ips
            print("logged_off_without_active",logged_off_without_active)
            loggedoff_flag=False
            sended=[]
            if logged_off_without_active != []:
                for sessdet in root.findall('.//subscription/session'):
                    for j in logged_off_without_active:
                        server_ip = sessdet.find('sessionServerIPAddress').text
                        server_name = sessdet.find('sessionServerName').text
                        if server_ip in logged_off_without_active:
                            select_query="select count(*) from targetconfigurations where deviceip='{}'".format(server_ip)
                            print(select_query)
                            curObj.execute(select_query)
                            retval = curObj.fetchall()
                            print(retval)
                            if retval[0][0]!=0:
                                deleteQuery="delete from targetconfigurations where deviceip='{}'".format(server_ip)
                                print(deleteQuery)
                                curObj.execute(deleteQuery)
                                conn.commit()
                                loggedoff_flag=True
                            else:
                                select_query="select count(*) from targetconfigurations where hostname='{}'".format(server_name)
                                print(select_query)
                                curObj.execute(select_query)
                                retval = curObj.fetchall()
                                if retval[0][0] != 0:
                                    deleteQuery="delete from targetconfigurations where hostname='{}'".format(server_name)
                                    curObj.execute(deleteQuery)
                                    conn.commit()
                                    loggedoff_flag=True
                            select_query="select count(*) from requestideventidmapper where requestid like '%{}'".format(server_ip)
                            curObj.execute(select_query)
                            retval = curObj.fetchall()
                            if retval[0][0] != 0:
                                deleteQuery="delete from requestideventidmapper where requestid like '%{}'".format(server_ip)
                                curObj.execute(deleteQuery)
                                conn.commit()
                                loggedoff_flag=True
                            else:
                                select_query="select count(*) from requestideventidmapper where requestid like '%{}'".format(server_name)
                                curObj.execute(select_query)
                                retval = curObj.fetchall()
                                if retval[0][0] != 0:
                                    deleteQuery="delete from requestideventidmapper where requestid like '%{}'".format(server_name)
                                    curObj.execute(deleteQuery)
                                    conn.commit()
                                    loggedoff_flag=True
        
                            if loggedoff_flag:
                                curenttime=datetime.now()
                                sent_resp_to_cm="<Resp><requestId /><result>SUCCESS</result><ipAddress>%s</ipAddress><hostName>%s</hostName><clearingTime>%s</clearingTime><responseMessage>Auto Removing Monitoring Target Processed Successfully!!</responseMessage></Resp>"
                                msgData=sent_resp_to_cm% (server_ip,server_name,curenttime)
                                sendMsgtoComm('12345678',22016, msgData, mbus)
                                loggedoff_flag=False
            return True
        #Check and update citrix session information from Api into table
        def checkCitrixSessionInformation(subscription,startDate, endDate,out):
            selquery = "select * from sessioninformation where username='{}' and logintime='{}'".format(out['username'],startDate)
            curObj.execute(selquery)
            retval = curObj.fetchall()
            #if out['ipaddress'] != 'No' or out['ipaddress'] != '':
            if (len(retval) == 0):
                generateXmlResponse(subscription,startDate, endDate, out)
                #insert data into table for newly created session
                InsertQuerySess="insert into sessioninformation(servername, serverip, username, clientip, protocol, clienttype, sessionid, logintime, disconnecttime, state, resourcepoolid) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"                           
                InsertData=(out['dnsname'], out['ipaddress'], out['username'], out['ClientAddress'], out['Protocol'], out['sessionServerType'], out['SessionKey'], startDate, endDate, out['ConnectionState'], out['resourcePoolId'])
                curObj.execute(InsertQuerySess, InsertData)
                conn.commit()
            else:
                generateXmlResponse(subscription,startDate, endDate, out)
                #Update sessions data into table for already existing endDate and status
                UpdateQuerySess="update sessioninformation set disconnecttime=%s, state=%s where username=%s and logintime=%s" 
                UpdateData=(endDate, out['ConnectionState'], out['username'], startDate) 
                curObj.execute(UpdateQuerySess, UpdateData)
                conn.commit()

            return


        def acquire_authtoken(tenantId, clientId, clientSecret, resource):
            authority_url = 'https://login.microsoftonline.com/' + tenantId
            context = adal.AuthenticationContext(authority_url, verify_ssl = False)
            authtoken = context.acquire_token_with_client_credentials(resource, clientId, clientSecret)
            return authtoken

        azure_mgmt_url = 'https://management.azure.com/'
        tenantid = message['tenantId']
        clientid = message['clientId']
        clientsecret = message['clientSecret']

        token = acquire_authtoken(tenantid, clientid, clientsecret, azure_mgmt_url)
        headers2 = {'Authorization': 'Bearer ' + token['accessToken'],'Content-Type': 'application/json'}
        azureSubUrl="https://management.azure.com/subscriptions/?api-version=2019-06-01"
        allSubscriptions =requests.get(azureSubUrl, headers=headers2).json()
        azureSubList=[]
        for azureSubData in allSubscriptions["value"]:
            #if azureSubData["displayName"]!= 'Startek-HUB':
            finalData={}
            finalData["subscriptionId"]=azureSubData["subscriptionId"]
            finalData["displayName"]=azureSubData["displayName"]
            azureSubList.append(finalData)
        atlogger.debug("fetched Azure SUBSCRIPTION LIST from API %s",finalData)


        resp = ET.Element('Resp')

        requestId = ET.SubElement(resp, "requestId")
        requestId.text = reqId

        tenantName = ET.SubElement(resp, "tenantName")
        tenantName.text = message['tenantName']

        result = ET.SubElement(resp, "result")
        result.text = "SUCCESS"

        errorCode = ET.SubElement(resp, "errorCode")
        errorCode.text = "0"

        responseType = ET.SubElement(resp, "responseType")
        responseType.text = "CitrixSessionDetails"

        responseMessage = ET.SubElement(resp, "responseMessage")
        responseMessage.text = "Successfully retrieved session information"

        responseMessage = ET.SubElement(resp, "currentTime")
        curdate=get_current_time()
        responseMessage.text = str(curdate)

        sessionInfo = ET.SubElement(resp, "sessionInfo")
          
        for azureData in azureSubList:
            splitName=azureData['displayName'].split("-")[-1]
            #atlogger.debug("SPLITED NAME:%s",splitName)
            subscription = ET.SubElement(sessionInfo, "subscription")
            subId = ET.SubElement(subscription, "subId")
            subId.text = azureData['subscriptionId']

            for hypervisorData in getHypervisorResponse:
                #if splitName in hypervisorData['Name']:
                for machineData in getMachinesResponse:
                    if str(hypervisorData['Id']) == str(machineData['hypervisorid']):
                        for sessionsData in getSessionResponse:
                            if str(machineData['id']) == str(sessionsData['MachineId']):
                                out={}
                                out['dnsname']=machineData['dnsname']
                                out['sessionServerType']='null'
                                #if machineData['ipaddress']!=None or machineData['ipaddress']!='':
                                out['ipaddress']=machineData['ipaddress'] if machineData['ipaddress']!=None else 'No'
                                out['SessionKey']=sessionsData['SessionKey']
                                out['StartDate']=sessionsData['StartDate']
                                out['EndDate']=sessionsData['EndDate']
                                        
                                if sessionsData['ConnectionState']==1:
                                    connectionState= "Connected"
                                elif sessionsData['ConnectionState']==6:
                                    connectionState= "Reconnected"
                                elif sessionsData['ConnectionState']==2:
                                    connectionState= "Disconnected"
                                elif sessionsData['ConnectionState']==5 or sessionsData['ConnectionState']==4:
                                    connectionState= "Active"
                                elif sessionsData['ConnectionState']==3:
                                    connectionState="Logged_Off"
            
                                out['ConnectionState']=connectionState
                                out['resourcePoolId'] = message['tenantName'] 
                                out['username'] = ''
                                out['Protocol'] = ''
                                out['ClientAddress'] = ''
                                for usersData in allUsersList:
                                    if str(sessionsData['UserId'])==str(usersData['id']):
                                        out['username']=usersData['username']
                                        break
                
                                for desktopgroupData in getDesktopgroupsResponse:
                                    if str(machineData['desktopgroupid'])==str(desktopgroupData['Id']):
                                        out['desktopgroupname']=desktopgroupData['Name']
                                        break
                                for clinetData in getConnectionsResponse:
                                    if str(sessionsData['CurrentConnectionId'])==str(clinetData['Id']):
                                        out['Protocol'] =clinetData['Protocol']
                                        out['ClientAddress']=clinetData['ClientAddress']                                  
                                        break
                                if out['StartDate'] is not None:
                                    startDate=out['StartDate'].replace("T", " ").replace("Z","")
            
                                if out['EndDate'] ==None:
                                    endDate=None
                                else:
                                    endDate=out['EndDate'].replace("T", " ").replace("Z","")
                                checkCitrixSessionInformation(subscription,startDate, endDate, out)
        response_xml = ET.tostring(resp)
        response_xml = response_xml.decode('utf-8')
        LoggedoffDelete(response_xml,mBus)
        msgType = 122002
        #reqId = 1234567890
        curObj.close()
        conn.close()
        atlogger.debug("final response for citrix session information is :%s",response_xml)
        #final_resp1 = ET.ElementTree(rootxml)
        sendMsgtoComm(reqId, msgType, response_xml, mBus)
        atlogger.debug("Transfer _to_ Analytics")
        transfer = transfer_to_analytics(message['tenantName'])

#converting datetime from UTC to IST 
def dateConversionFromUtcToIstFormat(utcDate):
    try:
        atlogger.debug("Entering into dateConversionFromUtcToIstFormat")
        format = "%Y-%m-%d %H:%M:%S"
        splitDate=utcDate.split(".")
        splitDateResult=splitDate[0].replace("T", " ")
        finalDateAfterConversion = datetime.strptime(splitDateResult, format)+ timedelta(hours=5,minutes=30)
        return finalDateAfterConversion
    except Exception as e:
        atlogger.debug('Returning Error from dateConversionFromUtcToIstFormat')
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        errMsg = "Exception in dateConversionFromUtcToIstFormat as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
        atlogger.error(errMsg) 

# Added for VMware Horizon Cloud on Azure V2
class horizonDetails:
    atlogger.debug("Entering into horizonDetails class")
    def __init__(self,subscriptionid,creds) :
        atlogger.debug("calling horizonDetails init function")  
        self.tenantid=creds.tenantid
        self.clientid = creds.applicationid
        self.clientsecret = creds.appsecret
        self.subscription_id = subscriptionid
        self.hzurl = creds.hzurl
        self.hzoauthid = creds.hzoauthid
        self.hzoauthsecret = creds.hzoauthsecret
        self.productType = creds.productType
        atlogger.debug("horizonDetails class parameters tenantid %s | clientid %s | clientsecret %s | subscription_id %s | hzurl %s | hzoauthsecret %s | hzoauthsecret  %s | productType %s |",self.tenantid,self.clientid ,self.clientsecret ,self.subscription_id,self.hzurl ,self.hzoauthid ,self.hzoauthsecret,self.productType)

    #generating Azure auth Token
    def fetchAzureAuthToken(self, resource):
        try:
            atlogger.debug("Entering into fetchAzureAuthToken")
            authority_url = 'https://login.microsoftonline.com/' + self.tenantid
            context = adal.AuthenticationContext(authority_url, verify_ssl = False)
            authtoken = context.acquire_token_with_client_credentials(resource, self.clientid, self.clientsecret)
            return authtoken
        except Exception as e:
            atlogger.debug('Returning Error from fetchAzureAuthToken')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in fetchAzureAuthToken as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)
    # Not in the right place
    def fetch_pagination_data(self, Url: str, total_pages: int):
        paginating_url = Url + "?page={}"
        atlogger.debug("Entering into fetchp_pagination_data")
        pagination_data_list = []
        for page_no in range(1, total_pages):
            pagination_url = paginating_url.format(page_no)
            pagination_response =requests.get(pagination_url,headers=self.authTokenheader, params="org_id={}".format(self.orgId))
            pagination_data = pagination_response.json()
            atlogger.debug("Response of the pagination %s",pagination_data)
            pagination_data_list.append(pagination_data["content"][0])
            atlogger.debug("Pagination Data list:%s",pagination_data_list)
        return pagination_data_list

        
    #Fetching  getAzureServersList List
    def getAzureServersList(self, azureSubUrl):
        try:
            atlogger.debug("Entering into getAzureServersList")
            azure_mgmt_url = 'https://management.azure.com/'
            token = self.fetchAzureAuthToken(azure_mgmt_url)
            atlogger.debug("getAzureServersList token is %s",token)
            headers2 = {'Authorization': 'Bearer ' + token['accessToken'],'Content-Type': 'application/json'}
            allSubscriptions =requests.get(azureSubUrl, headers=headers2).json() 
            atlogger.debug("fetched all subscriptions list from azure API %s",allSubscriptions)
            return allSubscriptions
        except Exception as e:
            atlogger.debug('Returning Error from getAzureServersList')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in getAzureServersList as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)
    
    def horizon_fetch(self):
        try:
            atlogger.debug("Entering into horizon_fetch function")
            #Generating authtoken using POST method
            atlogger.debug("Generating authtoken for horizon_fetch")
            tokenUrl = "https://console.cloud.vmware.com/csp/gateway/am/api/auth/authorize"
            headerType = {"Content-Type": "application/x-www-form-urlencoded"}
            payload = {"grant_type":"client_credentials"}
            atlogger.debug("getting horizon_fetch hzoauthid %s | hzoauthsecret %s |",self.hzoauthid,self.hzoauthsecret)
            tokenResponse = requests.post(tokenUrl, auth=(self.hzoauthid,self.hzoauthsecret), headers=headerType, data=payload)
            Bearertoken= tokenResponse.json()
            atlogger.debug("getting horizon_fetch Bearertoken %s",Bearertoken)
            token = "Bearer "+str(Bearertoken['access_token'])
            self.authTokenheader = {"Authorization": token}
        except Exception as e:
            atlogger.debug('Returning Error from horizon_fetch Bearer Token')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in horizon_fetch as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)

        try:
            #Fetching templates API
            rootUrl='cloud-sg-us.horizon.vmware.com'
            templateUrl="https://{}/admin/v2/templates".format(rootUrl)
            self.templateResponse = requests.get(templateUrl,  headers=self.authTokenheader)
            self.templatesData= self.templateResponse.json()
            template_total_page = self.templatesData['totalPages']
            if template_total_page > 1:
                template_pagination_data = self.fetch_pagination_data(templateUrl, template_total_page)
                self.templatesData['content'].extend(template_pagination_data)
            self.orgId=self.templatesData['content'][0]['orgId']
            atlogger.debug("Fetching orgId from templates API %s",self.orgId)
            atlogger.debug("Fetched templates API")
        except Exception as e:
            atlogger.debug('Returning Error from horizon_fetch templates API')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in horizon_fetch templates API as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)

        try:
            #Getching Azure virtualMachines API using subscription wise
            azureVmUrl='https://management.azure.com/subscriptions/{}/providers/Microsoft.Compute/virtualMachines?api-version=2018-06-01'.format(self.subscription_id)
            self.getVmResponse = self.getAzureServersList(azureVmUrl)
        except Exception as e:
            atlogger.debug('Returning Error from horizon_fetch virtualMachines API')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in horizon_fetch virtualMachines API as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)


        try:
            #Fetching Images API
            imagesUrl="https://{}/imagemgmt/v1/image-copies".format(rootUrl)
            atlogger.debug("Images url %s| headers  %s  | template %s |",imagesUrl,self.authTokenheader,self.orgId)
            self.imagesResponse = requests.get(imagesUrl,  headers=self.authTokenheader, params="org_id={}".format(self.orgId))
            atlogger.debug("imagesResponse from API %s",self.imagesResponse)
            self.imagesData= self.imagesResponse.json()
            total_page = self.imagesData['totalPages']
            atlogger.debug("Total Page Response:%s",str(total_page))
            if total_page > 1:
                pagination_data = self.fetch_pagination_data(imagesUrl, total_page)
                self.imagesData['content'].extend(pagination_data)
            atlogger.debug("Fetched Images API")
        except Exception as e:
            atlogger.debug('Returning Error from horizon_fetch virtualMachines API')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in horizon_fetch virtualMachines API as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)

        try:
            #Fetching entitlements API
            getEntitledments = 'https://{}/portal/v2/entitlements'.format(rootUrl)
            self.entitlementResponse = requests.get(getEntitledments,  headers=self.authTokenheader)
            atlogger.debug("entitlementResponse from API %s",self.entitlementResponse)
            self.entitlementData= self.entitlementResponse.json()
            atlogger.debug("Fetched entitlementData API")
            entitlement_total_page = self.entitlementData['totalPages']
            entitlement_page_no = 1
            if entitlement_total_page > 1:
                entitlement_data = self.fetch_pagination_data(getEntitledments, entitlement_total_page)
                self.entitlementData['content'].extend(entitlement_data)
                #for pageNo in range(1,entitlement_total_page):
                #    entitlement_data = fetch_pagination_data(getEntitledments, entitlement_total_page)
                #    page_entitlement_url = 'https://{}/portal/v2/entitlements?page={}'.format(rootUrl, entitlement_page_no)
                #    page_entitlement_response = requests.get(page_entitlement_url,headers=self.authTokenheader, params="org_id={}".format(self.orgId))
                #    pagination_entitlement_data = page_entitlement_response.json()
                #    self.entitlementData['content'].extend(pagination_entitlement_data['content'])
        except Exception as e:
            atlogger.debug('Returning Error from horizon_fetch virtualMachines API')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in horizon_fetch virtualMachines API as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)

    def to_xml(self,param):
        if param==4:
            atlogger.debug("Entering into Horizon to_xml function")
            try:
                if self.templateResponse.status_code == 200 and len(self.templatesData['content'])>0:
                    templates = xml.Element("templates")
                    for template in self.templatesData['content']:
                        atlogger.debug("Calculate the length of error Details:%s",self.templatesData['content'])
                        if len(template['reportedStatus']['errorDetails']) > 0:
                            continue
                        #Fetching Instances API
                        atlogger.debug("hzurl %s | template['providerInstanceId'] %s |",self.hzurl,template['providerInstanceId'])
                        instancesUrl="https://{}/admin/v2/providers/azure/instances/{}".format(self.hzurl,str(template['providerInstanceId']))
                        atlogger.debug("Instance URL:%s",instancesUrl)
                        instancesResponse = requests.get(instancesUrl,  headers=self.authTokenheader)
                        instancesData=instancesResponse.json()

                        atlogger.debug("Fetching Instances API Response %s",instancesData)

                        #Fetching VMS API
                        vmsUrl="https://{}/admin/v2/templates/{}/vms".format(self.hzurl, str(template['id']))
                        vmsResponse = requests.get(vmsUrl,  headers=self.authTokenheader)
                        vmsData= vmsResponse.json()
                        atlogger.debug("Fetching VMS API Response %s",vmsData)
                        vm_total_page = vmsData['totalPages']
                        if vm_total_page > 1:
                            vm_pagination_data = self.fetch_pagination_data(vmsUrl, vm_total_page)
                            vmsData['content'].extend(vm_pagination_data)
                        
                        if self.subscription_id ==str(instancesData['providerDetails']['data']['subscriptionId']):
                            templateInfo  = xml.Element("template")
                            templateInfo.append(xml_tag('id',template['id']))
                            templateInfo.append(xml_tag('name',template['name']))
                            templateInfo.append(xml_tag('orgId',str(template['orgId'])))
                            templateInfo.append(xml_tag('location',str(template['location'])))
                            templateInfo.append(xml_tag('vmNamePattern',str(template['vmNamePattern'])))
                            templateInfo.append(xml_tag('templateType',str(template['templateType'])))
                            templateInfo.append(xml_tag('templateConnectivityStatus',str(template['templateConnectivityStatus'])))
                            templateInfo.append(xml_tag('protocols',str(template['protocols'])))
                            templateInfo.append(xml_tag('vmLicenseType',str(template['vmLicenseType'])))
                            templates.append(templateInfo)

                            #Inserting templates into titancloudtemplatedetails table
                            try:
                                tempaltesQuery = session.query(TitanCloudTemplateDetails).filter_by(id = template['id'])
                                atlogger.debug("Entering into table titancloudtemplatedetails")
                                # if len(tempaltesQuery)==0:
                                if tempaltesQuery.count()==0:
                                    atlogger.debug("templates insertion started")
                                    titanTempaltes = TitanCloudTemplateDetails(id = template['id'],name = template['name'],orgid = template['orgId'],location = template['location'],templatetype = template['templateType'],templateconnectivitystatus = template['templateConnectivityStatus'],vmlicensetype = template['vmLicenseType'],sessionspervm =template['sessionsPerVm'],syncstatus = template['syncStatus'])
                                    session.add(titanTempaltes)
                                    session.commit()
                                    atlogger.debug("templates insertion completed")
                            except:
                                atlogger.debug("Record insertion into titancloudtemplatedetails failed ")
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                errMsg = "Exception in inserting templates  into table as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
                                atlogger.error(errMsg)
                                session.rollback()

                            virtualMachines = xml.Element("virtualMachines")
                            for vms in vmsData['content']:
                                virtualMachineInfo  = xml.Element("virtualMachine")
                                virtualMachineInfo.append(xml_tag('name',vms['id']))
                                virtualMachineInfo.append(xml_tag('lifecycleStatus',vms['lifecycleStatus']))
                                virtualMachineInfo.append(xml_tag('image',vms['image']))
                                virtualMachineInfo.append(xml_tag('privateIp',vms['privateIp']))
                                powerState="PowerOff" if vms['powerState'] =="PoweredOff" else "PowerOn"
                                virtualMachineInfo.append(xml_tag('powerState',powerState))
                                virtualMachineInfo.append(xml_tag('agentStatus',vms['agentStatus']))
                                virtualMachineInfo.append(xml_tag('agentVersion',vms['agentVersion']))
                                virtualMachineInfo.append(xml_tag('sessionPlacementStatus',vms['sessionPlacementStatus']))
                                virtualMachineInfo.append(xml_tag('maxSessions',vms['maxSessions']))
                                virtualMachineInfo.append(xml_tag('vmFreeSessions',vms['vmFreeSessions']))
                                virtualMachineInfo.append(xml_tag('createdAt',str(dateConversionFromUtcToIstFormat(str(vms['createdAt'])))))
                                virtualMachineInfo.append(xml_tag('updatedAt',str(dateConversionFromUtcToIstFormat(str(vms['updatedAt'])))))
                                virtualMachineInfo.append(xml_tag('sessions',vms['sessions']))
                                virtualMachines.append(virtualMachineInfo)

                                #Inserting Vms into titancloudvmsdetails table
                                try:
                                    vmsQuery = session.query(TitanCloudVmsDetails).filter_by(template_id = template['id'], name=vms['id'])
                                    atlogger.debug("Entering into titancloudvmsdetails")
                                    # if len(vmsQuery)==0:
                                    if vmsQuery.count()==0:
                                        atlogger.debug("vms insertion started")
                                        titanVms = TitanCloudVmsDetails(template_id = template['id'], name = vms['id'], lifecyclestatus = vms['lifecycleStatus'],   image = vms['image'], privateip = vms['privateIp'], powerstate = powerState, agentstatus = vms['agentStatus'], sessionplacementstatus =vms['sessionPlacementStatus'], maxsessions = vms['maxSessions'], vmfreesessions = vms['vmFreeSessions'])
                                        session.add(titanVms)
                                        session.commit()
                                        atlogger.debug("vms insertion completed")
                                except:
                                    atlogger.debug("Record insertion into titancloudvmsdetails failed ")
                                    exc_type, exc_obj, exc_tb = sys.exc_info()
                                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                    errMsg = "Exception in inserting vms  into table as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
                                    atlogger.error(errMsg)
                                    session.rollback()
    

                            templateInfo.append(virtualMachines)

                            infrastructureInfo  = xml.Element("infrastructure")
                            for infrastructureVmSkus in template['infrastructure']['vmSkus']:
                                vmSkus  = xml.Element("vmSkus")
                                vmSkus.append(xml_tag('kind',infrastructureVmSkus['kind']))
                                vmSkus.append(xml_tag('id',infrastructureVmSkus['id']))
                                infrastructureInfo.append(vmSkus)

                            diskSkus  = xml.Element("diskSkus")
                            if template['infrastructure'].get('diskSkus') != None:
                                for infrastructureDiskSkus in template['infrastructure']['diskSkus']:
                                    diskSkus.append(xml_tag('kind',infrastructureDiskSkus['kind']))
                                    diskSkus.append(xml_tag('id',infrastructureDiskSkus['id']))
                                    diskSkusData  = xml.Element("data")
                                    capabilities  = xml.Element("capabilities")
                                    capabilities.append(xml_tag('MaxSizeGiB',str(infrastructureDiskSkus['data']['capabilities']['MaxSizeGiB'])))
                                    capabilities.append(xml_tag('MaxValueOfMaxShares',str(infrastructureDiskSkus['data']['capabilities']['MaxValueOfMaxShares'])))
                                    capabilities.append(xml_tag('MinIOps',str(infrastructureDiskSkus['data']['capabilities']['MinIOps'])))
                                    capabilities.append(xml_tag('MinBandwidthMBps',str(infrastructureDiskSkus['data']['capabilities']['MinBandwidthMBps'])))
                                    capabilities.append(xml_tag('MaxBandwidthMBps',str(infrastructureDiskSkus['data']['capabilities']['MaxBandwidthMBps'])))
                                    capabilities.append(xml_tag('MaxIOps',str(infrastructureDiskSkus['data']['capabilities']['MaxIOps'])))
                                    capabilities.append(xml_tag('MinSizeGiB',str(infrastructureDiskSkus['data']['capabilities']['MinSizeGiB'])))
                                    diskSkusData.append(capabilities)
                                    diskSkus.append(diskSkusData)
                                infrastructureInfo.append(diskSkus)
                            templateInfo.append(infrastructureInfo)
                            
                            agentCustomization= xml.Element("agentCustomization")
                            #agentCustomization.append(xml_tag('runOnceCommand',str(template['agentCustomization']['runOnceCommand'])))
                            agentCustomization.append(xml_tag('disconnectSessionTimeoutMins',str(template['agentCustomization']['disconnectSessionTimeoutMins'])))
                            agentCustomization.append(xml_tag('emptySessionTimeoutMins',str(template['agentCustomization']['emptySessionTimeoutMins'])))
                            agentCustomization.append(xml_tag('emptySessionLogoffType',str(template['agentCustomization']['emptySessionLogoffType'])))
                            agentCustomization.append(xml_tag('demSettingId',str(template['agentCustomization']['demSettingId'])))
                            agentCustomization.append(xml_tag('sessionLoadBalancingSettings',str(template['agentCustomization']['sessionLoadBalancingSettings'])))
                            agentCustomization.append(xml_tag('skipDomainJoin',str(template['agentCustomization']['skipDomainJoin'])))
                            agentCustomization.append(xml_tag('esUrl',str(template['agentCustomization']['esUrl'])))
                            agentCustomization.append(xml_tag('proxyInfo',str(template['agentCustomization']['proxyInfo'])))
                            agentCustomization.append(xml_tag('dtemplateName',str(template['agentCustomization']['dtemplateName'])))
                            templateInfo.append(agentCustomization)

                            reportedStatus=xml.Element("reportedStatus")
                            reportedStatus.append(xml_tag('status',str(template['reportedStatus']['status'])))
                            reportedStatus.append(xml_tag('errorDetails',str(template['reportedStatus']['errorDetails'])))
                            reportedStatus.append(xml_tag('connectivityStatus',str(template['reportedStatus']['connectivityStatus'])))
                            reportedStatus.append(xml_tag('freeSessions',str(template['reportedStatus']['freeSessions'])))
                            reportedStatus.append(xml_tag('provisionedSessions',str(template['reportedStatus']['provisionedSessions'])))
                            reportedStatus.append(xml_tag('provisionedVMs',str(template['reportedStatus']['provisionedVMs'])))
                            reportedStatus.append(xml_tag('consumedVMs',str(template['reportedStatus']['consumedVMs'])))
                            reportedStatus.append(xml_tag('deletingVMs',str(template['reportedStatus']['deletingVMs'])))
                            reportedStatus.append(xml_tag('errorVMs',str(template['reportedStatus']['errorVMs'])))
                            reportedStatus.append(xml_tag('maintenanceVMs',str(template['reportedStatus']['maintenanceVMs'])))
                            reportedStatus.append(xml_tag('provisioningVMs',str(template['reportedStatus']['provisioningVMs'])))
                            reportedStatus.append(xml_tag('outdatedAgentVMs',str(template['reportedStatus']['outdatedAgentVMs'])))
                            reportedStatus.append(xml_tag('agentUpdateScanTime',str(template['reportedStatus']['agentUpdateScanTime'])))
                            reportedStatus.append(xml_tag('updatedAt',str(dateConversionFromUtcToIstFormat(str(template['reportedStatus']["updatedAt"])))))
                            reportedStatus.append(xml_tag('version',str(template['reportedStatus']['version'])))
                            templateInfo.append(reportedStatus)

                            sparePolicy  = xml.Element("sparePolicy")
                            sparePolicy.append(xml_tag('increment',str(template['sparePolicy']['increment'])))
                            sparePolicy.append(xml_tag('limit',str(template['sparePolicy']['limit'])))
                            sparePolicy.append(xml_tag('min',str(template['sparePolicy']['min'])))
                            sparePolicy.append(xml_tag('max',str(template['sparePolicy']['max'])))
                            templateInfo.append(sparePolicy)

                            templateInfo.append(xml_tag('sessionsPerVm',str(template['sessionsPerVm']))) 
                            templateInfo.append(xml_tag('syncStatus',str(template['syncStatus'])))
                            templateInfo.append(xml_tag('deleting',str(template['deleting'])))
                        
                    return templates
                elif self.templateResponse.status_code !=200:
                    atlogger.debug("Error occured while fetching templates API and  error code is : {}".format(self.templateResponse.status_code))
                else:
                    atlogger.debug("templates API response is returning empty")

            except Exception as e:
                atlogger.debug('Returning Error from to_xml')
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                errMsg = "Exception in to_xml as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
                atlogger.error(errMsg)
        
        #userProvisioning
        if param==5:
            atlogger.debug("Entering into Horizon UserProvisioning to_xml function")
            try:
                #Fetching Entitlement Api
                if self.entitlementResponse.status_code == 200 and len(self.entitlementData['content'])>0:
                    atlogger.debug("generating user provisioning xml")
                    horizonDetails = xml.Element("horizonDetails")
                    userProvisions = xml.Element("usersProvisioned")
                    for entitleitem in self.entitlementData['content']:
                        #Fetching pools Api
                        getPoolsforPid='https://{}/portal/v2/pools/'.format(self.hzurl)
                        getTempDetails=getPoolsforPid + entitleitem['poolIds'][0]
                        getTempReq =requests.get(getTempDetails,headers=self.authTokenheader)
                        getTempResponse=getTempReq.json()

                        for tempId in  getTempResponse['templates']:
                            #Fetching instance API to get subscriptions
                            getsubscriptionid= 'https://{}/admin/v2/providers/azure/instances/{}'.format(self.hzurl, str(tempId['providerInstance']['id']))
                            getSubcriptionIdreq =  requests.get(getsubscriptionid,headers=self.authTokenheader)
                            getSubcriptionIdres=getSubcriptionIdreq.json()
                            atlogger.debug("Type of subscription Details:%s",type(getSubcriptionIdres))
                            atlogger.debug("Subscription Details:%s",getSubcriptionIdres)
                            if getSubcriptionIdreq.status_code != 200:
                                continue
                            if self.subscription_id==str(getSubcriptionIdres['providerDetails']['data']['subscriptionId']):
                                user = xml.Element("user")
                                user.append(xml_tag('userPrincipalName',entitleitem['name']))
                                user.append(xml_tag('templateType',getTempResponse['templateType']))
                                user.append(xml_tag('templateId',tempId['id']))
                                userProvisions.append(user)
                                horizonDetails.append(userProvisions)
                    atlogger.debug("Fetched userprovisioning xml")
                    return horizonDetails
                elif self.entitlementResponse.status_code !=200:
                    atlogger.debug("Error occured while fetching userProvisioning API and  error code is : {}".format(self.entitlementResponse.status_code))
                else:
                    atlogger.debug("userProvisioning API response is returning empty")
            except Exception as e:
                atlogger.debug('Returning Error from UserProvisioning to_xml')
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                errMsg = "Exception in UserProvisioning to_xml as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
                atlogger.error(errMsg)


    def servers_xml(self,param):
        try:
            atlogger.debug('Entering into servers_xml')
            if param == 4:
                servers =  xml.Element("servers")
                atlogger.debug("Going to generate servers_xml")
                for vms in self.getVmResponse['value']:
                    if self.subscription_id in vms['id'] and "licenseType" not in vms['properties'] and vms['properties']['storageProfile']['osDisk']['osType'] != 'Linux':
                        #Calling servers API  to get powerState
                        serversPowerStateUrl='https://management.azure.com{}/instanceView?api-version=2018-06-01'.format(vms['id'])
                        getServersPsResponse = self.getAzureServersList(serversPowerStateUrl)
                        atlogger.debug("powerState fetched from the servers API")
                        serversPowerStates = {'VM running': 'PowerOn', 'VM deallocated': 'PowerOff', 'VM stopped': 'PowerOff'}
                        for serverStatus in getServersPsResponse['statuses']:
                            if "PowerState" in serverStatus['code']:
                                ServersDisplayStatus=serversPowerStates[serverStatus['displayStatus']]  
                                atlogger.debug("Servers API powerState :%s",ServersDisplayStatus)

                        server=xml.Element("server")
                        server.append(xml_tag('name',str(vms['name'])))
                        server.append(xml_tag('location',str(vms['location'])))
                        powerStateServer= str(ServersDisplayStatus) if ServersDisplayStatus !=None or ServersDisplayStatus !='' else ''
                        server.append(xml_tag('powerState',powerStateServer))

                        server.append(xml_tag('vmSize',str(vms['properties']['hardwareProfile']['vmSize'])))
                        server.append(xml_tag('osType',str(vms['properties']['storageProfile']['osDisk']['osType'])))
                        mndata=vms['properties']['networkProfile']['networkInterfaces']
                        if 'deleteOption' in mndata:
                            for deleteoption in mndata:
                                server.append(xml_tag('deleteOption',str(deleteoption['properties']['deleteOption'])))
                        else:
                            server.append(xml_tag('deleteOption',None))
                        disksizegb=vms['properties']['storageProfile']['osDisk']
                        if 'diskSizeGB' in disksizegb:
                            server.append(xml_tag('diskSizeGB',str(disksizegb['diskSizeGB'])))
                        else:
                            server.append(xml_tag('diskSizeGB',None))
                        servers.append(server)
                atlogger.debug("Fetched servers_xml List")
                return servers
        except Exception as e:
            atlogger.debug('Returning Error from servers_xml')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in servers_xml as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)

    def images_xml(self,param):
        try:
            atlogger.debug("Entering into images_xml function")
            if param == 4:
                if self.imagesResponse.status_code == 200 and len(self.imagesData['content'])>0:
                    images = xml.Element("images")
                    atlogger.debug("Going to generate images_xml:%s",self.imagesData['content'])
                    for imagesList in self.imagesData['content']:
                        atlogger.debug("Image List:%s",imagesList)
                        if self.subscription_id in imagesList['assetDetails']['data']['resourceId']:
                            image = xml.Element("image")
                            image.append(xml_tag("id",str(imagesList['id'])))
                            image.append(xml_tag("orgId",str(imagesList['orgId'])))
                            image.append(xml_tag("name",str(imagesList['assetDetails']['data']['galleryImageName'])))
                            image.append(xml_tag("multiSession",str(imagesList['assetDetails']['data']['multiSession'])))
                            image.append(xml_tag("osType",str(imagesList['assetDetails']['data']['osType'])))
                            image.append(xml_tag("status",str(imagesList['status'])))
                            image.append(xml_tag("versionCount",str(imagesList['version'])))
                            image.append(xml_tag("createdAt",str(dateConversionFromUtcToIstFormat(str(imagesList['createdAt'])))))
                            image.append(xml_tag("updatedAt",str(dateConversionFromUtcToIstFormat(str(imagesList['updatedAt'])))))
                            image.append(xml_tag("version",str(imagesList['assetDetails']['data']['galleryImageVersionName'])))
                            images.append(image)
                    atlogger.debug("Fetched images List")
                    return images
                elif self.imagesResponse.status_code !=200:
                    atlogger.debug("Error occured while fetching Images API and  error code is : {}".format(self.imagesResponse.status_code))
                else:
                    atlogger.debug("Images API response is returning empty")
        except Exception as e:
            atlogger.debug('Returning Error from images_xml')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in images_xml as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)
                
    def uag_xml(rootUrl,authTokenheader):
        try:
            atlogger.debug("Fetching uag-deployments API")
            #Fetching uag-deployments API
            uagDeploymentsUrl="https://{}/admin/v2/uag-deployments".format(rootUrl)
            uagDeploymentsResponse = requests.get(uagDeploymentsUrl,  headers=authTokenheader)
            uagDeploymentsData= uagDeploymentsResponse.json()
            uag_total_page = uagDeploymentsData['totalPages']
            if uag_total_page > 1:
                uag_pagination_data = self.fetch_pagination_data(uagDeploymentsUrl, uag_total_page)
                uagDeploymentsData['content'].extend(uag_pagination_data)
            atlogger.debug("Enterering into uag_xml generation")
            if uagDeploymentsResponse.status_code == 200 and len(uagDeploymentsData['content'])>0:
                uagDeploymentInfo = xml.Element("uagDeployment")
                atlogger.debug("Going to generate uag_xml")
                for uagDeploymentList in uagDeploymentsData['content']:
                    uagDeploymentInfo.append(xml_tag("id",str(uagDeploymentList['id'])))
                    uagDeploymentInfo.append(xml_tag("name",str(uagDeploymentList['name'])))
                    uagDeploymentInfo.append(xml_tag("description",str(uagDeploymentList['description'])))
                    uagDeploymentInfo.append(xml_tag("orgId",str(uagDeploymentList['orgId'])))
                    uagDeploymentInfo.append(xml_tag("location",str(uagDeploymentList['location'])))
                    uagDeploymentInfo.append(xml_tag("type",str(uagDeploymentList['type'])))
                    uagDeploymentInfo.append(xml_tag("providerLabel",str(uagDeploymentList['providerLabel'])))
                    uagDeploymentInfo.append(xml_tag("status",str(uagDeploymentList['status'])))
                    uagDeploymentInfo.append(xml_tag("fqdn",str(uagDeploymentList['fqdn'])))
                    uagDeploymentInfo.append(xml_tag("numberOfGateways",str(uagDeploymentList['numberOfGateways'])))
                    gateways=xml.Element("gateways")
                    for gatewayInfo in uagDeploymentList['gateways']:
                        gatewayList=xml.Element("gateway")
                        gatewayList.append(xml_tag("id",str(gatewayInfo['id'])))
                        gatewayList.append(xml_tag("managementPrivateIp",str(gatewayInfo['managementPrivateIp'])))
                        gatewayList.append(xml_tag("desktopPrivateIp",str(gatewayInfo['desktopPrivateIp'])))
                        gatewayList.append(xml_tag("dmzPrivateIp",str(gatewayInfo['dmzPrivateIp'])))
                        gatewayList.append(xml_tag("vmName",str(gatewayInfo['vmName'])))
                        gatewayList.append(xml_tag("version",str(gatewayInfo['version'])))
                        gatewayList.append(xml_tag("errorDetails",str(gatewayInfo['errorDetails'])))
                        gatewayList.append(xml_tag("status",str(gatewayInfo['status'])))
                        gatewayList.append(xml_tag("markedForDeletion",str(gatewayInfo['markedForDeletion'])))
                        gateways.append(gatewayList)
                    uagDeploymentInfo.append(gateways)  
                    sslCertificateTO=xml.Element("sslCertificateTO")
                    sslCertificateTO.append(xml_tag("credentialId",str(uagDeploymentList['sslCertificateTO']['credentialId'])))
                    sslCertificateTO.append(xml_tag("expiryDate",str(dateConversionFromUtcToIstFormat(str(uagDeploymentList['sslCertificateTO']['expiryDate'])))))
                    sslCertificateTO.append(xml_tag("sslCertificateType",str(uagDeploymentList['sslCertificateTO']['sslCertificateType'])))
                    uagDeploymentInfo.append(sslCertificateTO)
                    uagDeploymentInfo.append(xml_tag("createdAt",str(dateConversionFromUtcToIstFormat(str(uagDeploymentList['createdAt'])))))
                    uagDeploymentInfo.append(xml_tag("updatedAt",str(dateConversionFromUtcToIstFormat(str(uagDeploymentList['updatedAt'])))))
                    uagDeploymentInfo.append(xml_tag("version",str(uagDeploymentList['version'])))
                    loadBalancer=xml.Element("loadBalancer")
                    loadBalancer.append(xml_tag("ipAddress", str(uagDeploymentList['loadBalancer']['ipAddress'])))
                    loadBalancer.append(xml_tag("fqdn",str(uagDeploymentList['loadBalancer']['fqdn'])))
                    loadBalancer.append(xml_tag("publicIpEnabled",str(uagDeploymentList['loadBalancer']['publicIpEnabled'])))
                    loadBalancer.append(xml_tag("empty",str(uagDeploymentList['loadBalancer']['empty']))) 
                    uagDeploymentInfo.append(loadBalancer)     
                atlogger.debug("Finished uag_xml List")
                return uagDeploymentInfo
            elif uagDeploymentsResponse.status_code !=200:
                atlogger.debug("Error occured while fetching Uag-Deployments API and  error code is : {}".format(uagDeploymentsResponse.status_code))
            else:
                atlogger.debug("Uag-Deployments API response is returning empty")
        except Exception as e:
            atlogger.debug('Returning Error from uag_xml')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in uag_xml as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)                

    def edge_xml(rootUrl,authTokenheader):
        try:  
            atlogger.debug("Fetching edge-deployments API")
            edgeUrl='https://{}/admin/v2/edge-deployments'.format(rootUrl)
            edgeResponse = requests.get(edgeUrl,headers=authTokenheader)
            edgeData = edgeResponse.json()
            edge_total_page = edgeData['totalPages']
            if edge_total_page > 1:
                edge_pagination_data = self.fetch_pagination_data(edgeUrl, edge_total_page)
                edgeData['content'].extend(edge_pagination_data)
            atlogger.debug("Enterering into edge_xml generation")
            if edgeResponse.status_code == 200 and len(edgeData['content'])>0:
                egdeInfo=xml.Element('edgeDeployment')
                for edge in edgeData['content']:
                    egdeInfo.append(xml_tag("id",str(edge['id'])))
                    egdeInfo.append(xml_tag("orgId",str(edge['orgId'])))
                    egdeInfo.append(xml_tag("location",str(edge['location'])))
                    egdeInfo.append(xml_tag("name",str(edge['name'])))
                    egdeInfo.append(xml_tag("description",str(edge['description'])))
                    egdeInfo.append(xml_tag("status",str(edge['status'])))
                    egdeInfo.append(xml_tag("fqdn",str(edge['fqdn'])))
                    egdeInfo.append(xml_tag("edgeVmIP",str(edge['edgeVmDetails']['managementIp'])))
                    egdeInfo.append(xml_tag("adTwinSyncStatus",str(edge['adTwinSyncStatus'])))
                    adMonitoring=xml.Element('adMonitoring')
                    adMonitoring.append(xml_tag("enabled",str(edge['adMonitoring']['enabled'])))
                    adMonitoring.append(xml_tag("interval",str(edge['adMonitoring']['interval'])))
                    statusMap=xml.Element("statusMap")
                    status=edge['adMonitoring'].get('statusMap')
                    if status:
                        for i,j in status.items():
                            statusMap.append(xml_tag("status",str(j['status'])))
                            statusMap.append(xml_tag("message",str(j['message'])))
                            adMonitoring.append(statusMap)
                    egdeInfo.append(adMonitoring) 
                    egdeInfo.append(xml_tag("version",str(edge['version'])))
                    egdeInfo.append(xml_tag("createdAt",str(dateConversionFromUtcToIstFormat(str(edge['createdAt'])))))
                    egdeInfo.append(xml_tag("updatedAt",str(dateConversionFromUtcToIstFormat(str(edge['updatedAt'])))))
                atlogger.debug("Fininshed edge_xml List")    
                return egdeInfo
            elif edgeResponse.status_code !=200:
                atlogger.debug("Error occured while fetching Edge-Deployments API and  error code is : {}".format(edgeResponse.status_code))
            else:
                atlogger.debug("Edge-Deployments API response is returning empty")
        except Exception as e:
            atlogger.debug('Returning Error from uag_xml')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in uag_xml as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)   

    def apps_xml():
        apps= xml.Element('apps')
        app= xml.Element('app')
        emptyTags=''
        if emptyTags!='':
            app.append(xml_tag('name',emptyTags))
            app.append(xml_tag('id',emptyTags))
            app.append(xml_tag('version',emptyTags))
            app.append(xml_tag('orgId',emptyTags))
            app.append(xml_tag('os',emptyTags))
            app.append(xml_tag('path',emptyTags))
            app.append(xml_tag('publisher',emptyTags))
            app.append(xml_tag('templateName',emptyTags))
        apps.append(app)
        return apps
    
    def usersList_xml(rootUrl,authTokenheader):
        try:
            atlogger.debug("Fetching users list API")

            #Fetching users list API
            usersUrl="https://{}/auth/v1/admin/users".format(rootUrl)
            usersResponse = requests.get(usersUrl,  headers=authTokenheader)
            usersData= usersResponse.json()
            user_total_page = usersData['totalPages']
            if user_total_page > 1:
                user_pagination_data = self.fetch_pagination_data(usersUrl, user_total_page)
                usersData['content'].extend(user_pagination_data)
            atlogger.debug("Enterering into usersList_xml generation")

            if usersResponse.status_code == 200 and len(usersData['users'])>0:
                users = xml.Element("users")
                for usersList in usersData['users']:
                    userInfo = xml.Element("user")
                    userInfo.append(xml_tag("userPrincipalName",str(usersList['userPrincipalName'])))
                    userInfo.append(xml_tag("id",str(usersList['id'])))
                    users.append(userInfo)
                atlogger.debug("Finished usersList_xml List")
                return users
            elif usersResponse.status_code !=200:
                atlogger.debug("Error occured while fetching usersList_xml API and  error code is : {}".format(usersResponse.status_code))
            else:
                atlogger.debug("usersList_xml API response is returning empty")
        except Exception as e:
            atlogger.debug('Returning Error from user_xml')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            errMsg = "Exception in uag_xml as %s %s %s %s"  % (e, exc_type, fname, str(exc_tb.tb_lineno))
            atlogger.error(errMsg)
