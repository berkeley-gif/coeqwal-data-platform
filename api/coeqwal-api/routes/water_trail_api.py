"""
Water Trail API - Connect the dots approach for legible CalSim network visualization
Uses hardcoded water pathways based on actual network_topology.csv connectivity
Focuses on major reservoirs and their connections to Sacramento/San Joaquin river systems
"""

import asyncpg
from typing import List, Dict, Any, Set
import json

# HARDCODED CALIFORNIA WATER TRAILS based on actual network topology
# These represent the most important water infrastructure pathways

CALIFORNIA_WATER_TRAILS = {
    "shasta_sacramento": {
        "name": "Shasta Dam to Sacramento River System",
        "description": "Northern California's primary water source - MASSIVE COMPREHENSIVE pathway",
        "nodes": [
            # ALL NORTHERN CALIFORNIA RESERVOIRS (68 total)
            "SHSTA", "KSWCK", "LWSTN", "TRNTY", "WKYTN", "OROVL", "ENGLB", "BLKBT", "SGRGE", "EPARK",
            "FOLSM", "NTOMA", "CMPFW", "CMBIE", "CLRLK", "BRYSA", "SPLDG", "MERLC", "BOWMN", "LBEAR",
            "LGRSV", "LKVLY", "LOONL", "LOSVQ", "MCLRE", "MLRTN", "MNRRH", "MTMDW", "NBLDB", "NHGAN",
            "PARDE", "RLLNS", "RVPHB", "SCOTF", "SILVR", "SLTSP", "SLYCK", "STMMD", "TBAUD", "THRMA",
            "THRMF", "TRLCH", "TULOC", "UNVLY", "WDWRD", "FRDYC", "FRMAN", "FRMDW", "HHOLE", "ICEHS",
            "INDVL", "JKSMD", "JNKSN", "DAVIS", "ESTMN", "CAPLS", "BTVLY", "ALMNR", "AMADR", "BANOS",
            "HNSLY", "PEDRO", "CMCHE", "MDSTO", "MELON",
            
            # ALL NORTHERN CALIFORNIA TREATMENT PLANTS (39 WTPs + 22 WWTPs)
            "WTPAMC", "WTPAUB", "WTPBJM", "WTPBLV", "WTPBNC", "WTPBTB", "WTPBUK", "WTPBWM", "WTPCMT",
            "WTPCOL", "WTPCSD", "WTPDEF", "WTPDGT", "WTPDWP", "WTPDWS", "WTPEDH", "WTPELD", "WTPFBN",
            "WTPFMH", "WTPFOL", "WTPFSS", "WTPFTH", "WTPJAC", "WTPJJO", "WTPJMS", "WTPJYL", "WTPMNR",
            "WTPMOD", "WTPNBR", "WTPOPH", "WTPRSV", "WTPSAC", "WTPSJP", "WTPSRW", "WTPTAB", "WTPVNY",
            "WTPWAL", "WTPWDH", "WTPWMN", "AWWWTP", "CCWWTP", "CHWWTP", "DCWWTP", "DVWWTP", "EAWWTP",
            "EDWWTP", "LCWWTP", "MAWWTP", "MDWWTP", "MOWWTP", "ORWWTP", "PGWWTP", "SRWWTP", "STWWTP",
            "SWWWTP", "TCWWTP", "TKWWTP", "WLWWTP", "WSWWTP", "WTPCYC", "YCWWTP",
            
            # ALL PUMP STATIONS
            "DWRPS1", "DWRPS2", "RD1500",
            
            # Complete Shasta to Sacramento pathway with ALL connecting arcs
            "C_SHSTA", "SAC301", "C_SAC301", "C_KSWCK", 
            "SAC299", "C_SAC299", "SAC296", "C_SAC296", "SAC294", "C_SAC294", 
            "SAC289", "C_SAC289", "SAC287", "C_SAC287", "SAC281", "C_SAC281", 
            "SAC277", "C_SAC277", "SAC275", "C_SAC275", "SAC273", "C_SAC273", 
            "SAC271", "C_SAC271", "SAC269", "C_SAC269", "SAC267", "C_SAC267", 
            "SAC265", "C_SAC265", "SAC263", "C_SAC263", "SAC261", "C_SAC261", 
            "SAC259", "C_SAC259", "SAC257", "C_SAC257", "SAC255", "C_SAC255", 
            "SAC253", "C_SAC253", "SAC251", "C_SAC251", "SAC249", "C_SAC249", 
            "SAC247", "C_SAC247", "SAC245", "C_SAC245", "SAC243", "C_SAC243", 
            "SAC241", "C_SAC241", "SAC239", "C_SAC239", "SAC237", "C_SAC237", 
            "SAC235", "C_SAC235", "SAC233", "C_SAC233", "SAC232", "C_SAC232", 
            "SAC230", "C_SAC230", "SAC228", "C_SAC228", "SAC226", "C_SAC226", 
            "SAC224", "C_SAC224", "SAC222", "C_SAC222", "SAC220", "C_SAC220", 
            "SAC218", "C_SAC218", "SAC216", "C_SAC216", "SAC214", "C_SAC214", 
            "SAC212", "C_SAC212", "SAC210", "C_SAC210", "SAC208", "C_SAC208", 
            "SAC207", "C_SAC207", "SAC205", "C_SAC205", "SAC203", "C_SAC203", 
            "SAC201", "C_SAC201", "SAC199", "C_SAC199", "SAC197", "C_SAC197", 
            "SAC196", "C_SAC196", "SAC194", "C_SAC194", "SAC193", "C_SAC193", 
            "SAC191", "C_SAC191", "SAC189", "C_SAC189", "SAC187", "C_SAC187", 
            "SAC185", "C_SAC185", "SAC184", "C_SAC184", "SAC182", "C_SAC182", 
            "SAC180", "C_SAC180", "SAC178", "C_SAC178", "SAC176", "C_SAC176", 
            "SAC174", "C_SAC174", "SAC172", "C_SAC172", "SAC170", "C_SAC170", 
            "SAC168", "C_SAC168", "SAC166", "C_SAC166", "SAC164", "C_SAC164", 
            "SAC162", "C_SAC162", "SAC160", "C_SAC160", "SAC159", "C_SAC159", 
            "SAC157", "C_SAC157", "SAC155", "C_SAC155", "SAC154", "C_SAC154", 
            "SAC152", "C_SAC152", "SAC150", "C_SAC150", "SAC148", "C_SAC148", 
            "SAC146", "C_SAC146", "SAC144", "C_SAC144", "SAC143", "C_SAC143", 
            "SAC141", "C_SAC141", "SAC139", "C_SAC139", "SAC137", "C_SAC137", 
            "SAC135", "C_SAC135", "SAC134", "C_SAC134", "SAC132", "C_SAC132", 
            "SAC130", "C_SAC130", "SAC129", "C_SAC129", "SAC127", "C_SAC127", 
            "SAC125", "C_SAC125", "SAC124", "C_SAC124", "SAC122", "C_SAC122", 
            "SAC120", "C_SAC120", "SAC119", "C_SAC119", "SAC117", "C_SAC117", 
            "SAC115", "C_SAC115", "SAC113", "C_SAC113", "SAC111", "C_SAC111", 
            "SAC109", "C_SAC109", "SAC107", "C_SAC107", "SAC105", "C_SAC105", 
            "SAC103", "C_SAC103", "SAC101", "C_SAC101", "SAC099", "C_SAC099", 
            "SAC097", "C_SAC097", "SAC095", "C_SAC095", "SAC093", "C_SAC093", 
            "SAC091", "C_SAC091", "SAC089", "C_SAC089", "SAC087", "C_SAC087", 
            "SAC085", "C_SAC085", "SAC083", "C_SAC083", "SAC081", "C_SAC081", 
            "SAC079", "C_SAC079", "SAC077", "C_SAC077", "SAC075", "C_SAC075", 
            "SAC073", "C_SAC073", "SAC071", "C_SAC071", "SAC069", "C_SAC069", 
            "SAC067", "C_SAC067", "SAC065", "C_SAC065", "SAC063", "C_SAC063", 
            "SAC061", "C_SAC061", "SAC059", "C_SAC059", "SAC057", "C_SAC057", 
            "SAC055", "C_SAC055", "SAC053", "C_SAC053", "SAC051", "C_SAC051", 
            "SAC050", "C_SAC050", "SAC048", "C_SAC048", "SAC047", "C_SAC047", 
            "SAC045", "C_SAC045", "SAC043", "C_SAC043", "SAC041", "C_SAC041", 
            "SAC039", "C_SAC039", "SAC037", "C_SAC037", "SAC035", "C_SAC035", 
            "SAC033", "C_SAC033", "SAC031", "C_SAC031", "SAC030", "C_SAC030", 
            "SAC029", "C_SAC029", "SAC027", "C_SAC027", "SAC025", "C_SAC025", 
            "SAC023", "C_SAC023", "SAC021", "C_SAC021", "SAC019", "C_SAC019", 
            "SAC017", "C_SAC017", "SAC015", "C_SAC015", "SAC013", "C_SAC013", 
            "SAC011", "C_SAC011", "SAC009", "C_SAC009", "SAC007", "C_SAC007", 
            "SAC005", "C_SAC005", "SAC003", "C_SAC003", "SAC001", "C_SAC001", "SAC000"
        ],
        "key_infrastructure": ["SHSTA", "KSWCK", "OROVL", "FOLSM", "SAC083", "SAC043", "SAC000"],
        "region": "SAC"
    },
    
    "oroville_feather": {
        "name": "Oroville Dam to Feather River to Sacramento - COMPREHENSIVE",
        "description": "MASSIVE Central Valley water supply via Feather River with ALL tributaries and infrastructure",
        "nodes": [
            # COMPLETE FEATHER RIVER SYSTEM - All FTR nodes
            "OROVL", "THRMF", "THRMA", "FTR072", "C_FTR072", "FTR070", "C_FTR070", "FTR068", "C_FTR068", 
            "FTR067", "C_FTR067", "FTR065", "C_FTR065", "FTR063", "C_FTR063", "FTR061", "C_FTR061", 
            "FTR059", "C_FTR059", "FTR057", "C_FTR057", "FTR055", "C_FTR055", "FTR053", "C_FTR053", 
            "FTR051", "C_FTR051", "FTR049", "C_FTR049", "FTR048", "C_FTR048", "FTR047", "C_FTR047", 
            "FTR045", "C_FTR045", "FTR043", "C_FTR043", "FTR041", "C_FTR041", "FTR039", "C_FTR039", 
            "FTR037", "C_FTR037", "FTR036", "C_FTR036", "FTR035", "C_FTR035", "FTR033", "C_FTR033", 
            "FTR031", "C_FTR031", "FTR030", "C_FTR030", "FTR029", "C_FTR029", "FTR028", "C_FTR028", 
            "FTR027", "C_FTR027", "FTR025", "C_FTR025", "FTR023", "C_FTR023", "FTR021", "C_FTR021", 
            "FTR019", "C_FTR019", "FTR018", "C_FTR018", "FTR017", "C_FTR017", "FTR016", "C_FTR016", 
            "FTR015", "C_FTR015", "FTR014", "C_FTR014", "FTR013", "C_FTR013", "FTR011", "C_FTR011", 
            "FTR009", "C_FTR009", "FTR008", "C_FTR008", "FTR007", "C_FTR007", "FTR005", "C_FTR005", 
            "FTR003", "C_FTR003", "SAC083",  # Feather joins Sacramento
            
            # YUBA RIVER SYSTEM - Complete tributary
            "ENGLB", "YUB001", "YUB005", "YUB010", "YUB015", "YUB020", "YUB025", "YUB030",
            "C_YUB001", "C_YUB005", "C_YUB010", "C_YUB015", "C_YUB020", "C_YUB025", "C_YUB030",
            
            # BEAR RIVER SYSTEM - Complete tributary  
            "CMPFW", "CMBIE", "LBEAR", "BRR001", "BRR005", "BRR010", "BRR015", "BRR020",
            "C_BRR001", "C_BRR005", "C_BRR010", "C_BRR015", "C_BRR020"
        ],
        "key_infrastructure": ["OROVL", "ENGLB", "CMPFW", "FTR003", "SAC083"],
        "region": "SAC"
    },
    
    "folsom_american": {
        "name": "Folsom Lake to American River to Sacramento - COMPREHENSIVE",
        "description": "MASSIVE American River system with ALL tributaries, reservoirs, and infrastructure",
        "nodes": [
            # COMPLETE AMERICAN RIVER SYSTEM - All AMR nodes and tributaries
            "FOLSM", "NTOMA", "AMR028", "C_AMR028", "AMR026", "C_AMR026", "AMR024", "C_AMR024", 
            "AMR022", "C_AMR022", "AMR020", "C_AMR020", "AMR018", "C_AMR018", "AMR016", "C_AMR016", 
            "AMR014", "C_AMR014", "AMR012", "C_AMR012", "AMR010", "C_AMR010", "AMR008", "C_AMR008", 
            "AMR006", "C_AMR006", "AMR007", "C_AMR007", "AMR004", "C_AMR004", "AMR002", "C_AMR002", 
            "SAC043",  # American joins Sacramento
            
            # SOUTH FORK AMERICAN RIVER - Complete tributary system
            "SFA001", "SFA005", "SFA010", "SFA015", "SFA020", "SFA025", "SFA030", "SFA035", "SFA040",
            "C_SFA001", "C_SFA005", "C_SFA010", "C_SFA015", "C_SFA020", "C_SFA025", "C_SFA030",
            "ICEHS", "RVPHB", "UNVLY", "LOONL", "SILVR",  # South Fork reservoirs
            
            # MIDDLE FORK AMERICAN RIVER - Complete tributary system  
            "MFA001", "MFA005", "MFA010", "MFA015", "MFA020", "MFA025", "MFA030",
            "C_MFA001", "C_MFA005", "C_MFA010", "C_MFA015", "C_MFA020", "C_MFA025",
            "FRDYC", "FRMAN", "FRMDW",  # Middle Fork reservoirs
            
            # NORTH FORK AMERICAN RIVER - Complete tributary system
            "NFA001", "NFA005", "NFA010", "NFA015", "NFA020", "NFA025", "NFA030",
            "C_NFA001", "C_NFA005", "C_NFA010", "C_NFA015", "C_NFA020", "C_NFA025",
            "BOWMN", "SPLDG", "SCOTF",  # North Fork reservoirs
            
            # AMERICAN RIVER TREATMENT PLANTS - All WTPs in basin
            "WTPEDH", "WTPSJP", "WTPRSV", "WTPFOL", "WTPSAC", "WTPFSS", "WTPAUB", "WTPFTH"
        ],
        "key_infrastructure": ["FOLSM", "NTOMA", "ICEHS", "BOWMN", "SAC043"],
        "region": "SAC"
    },
    
    "san_luis_delta": {
        "name": "San Luis Reservoir to Delta System",
        "description": "Central Valley Project water distribution",
        "nodes": [
            "SLUIS", "SLUISC", "DMC", "CALA", "MENDOTA", "SJR070", "SJR062",
            "SJR056", "SJR053", "SJR048", "SJR043", "SJR042", "SJR038", "SJR033",
            "SJR028", "SJR026", "SJR023", "SJR022", "SJR013", "SJR009", "MDOTA",
            "SJRW", "SJRE"
        ],
        "key_infrastructure": ["SLUIS", "MENDOTA", "MDOTA", "SJRE"],
        "region": "SJR"
    },
    
    "hetch_hetchy_tuolumne": {
        "name": "Hetch Hetchy to Tuolumne River to San Joaquin",
        "description": "San Francisco water supply and Central Valley",
        "nodes": [
            "HETCH", "DMPDN", "EXCQR", "TULLK", "MODTO", "TUO054", "TUO040",
            "TUO026", "TUO009", "SJR070"  # Tuolumne joins San Joaquin
        ],
        "key_infrastructure": ["HETCH", "TULLK", "MODTO", "SJR070"],
        "region": "SJR"
    },
    
    "trinity_sacramento": {
        "name": "Trinity River to Sacramento via Whiskeytown",
        "description": "North Coast water transfer to Central Valley",
        "nodes": [
            "TRNTY", "WKYTN", "KSWCK", "SAC299"  # Trinity transfers to Sacramento system
        ],
        "key_infrastructure": ["TRNTY", "WKYTN", "KSWCK"],
        "region": "SAC"
    },
    
    # SOUTH OF DELTA SYSTEMS
    
    "central_valley_project_south": {
        "name": "Central Valley Project - South of Delta - MASSIVE COMPREHENSIVE",
        "description": "COMPLETE CVP water distribution to San Joaquin Valley and Southern California with ALL infrastructure",
        "nodes": [
            # CRITICAL DELTA PUMPING - Jones Pumping Plant
            "DMC003", "OMR028", "OMR027",  # Jones PP and Delta diversion points
            
            # San Luis to Mendota Pool - COMPLETE SYSTEM
            "SLUIS", "SLUISC", "DMC", "CALA", "MENDOTA", "MDOTA",
            "DMC001", "DMC002", "DMC004", "DMC005", "DMC006", "DMC007", "DMC008", "DMC009", "DMC010",
            "DMC011", "DMC012", "DMC013", "DMC014", "DMC015", "DMC016", "DMC017", "DMC018", "DMC019", "DMC020",
            "C_DMC001", "C_DMC002", "C_DMC003", "C_DMC004", "C_DMC005", "C_DMC006", "C_DMC007", "C_DMC008",
            "C_DMC009", "C_DMC010", "C_DMC011", "C_DMC012", "C_DMC013", "C_DMC014", "C_DMC015", "C_DMC016",
            
            # Friant-Kern Canal system - COMPLETE 150+ mile canal
            "MILLR", "FKC001", "FKC002", "FKC003", "FKC004", "FKC005", "FKC006", "FKC007", "FKC008", "FKC009", "FKC010",
            "FKC011", "FKC012", "FKC013", "FKC014", "FKC015", "FKC016", "FKC017", "FKC018", "FKC019", "FKC020",
            "FKC021", "FKC022", "FKC023", "FKC024", "FKC025", "FKC026", "FKC027", "FKC028", "FKC029", "FKC030",
            "FKC031", "FKC032", "FKC033", "FKC034", "FKC035", "FKC036", "FKC037", "FKC038", "FKC039", "FKC040",
            "FKC041", "FKC042", "FKC043", "FKC044", "FKC045", "FKC046", "FKC047", "FKC048", "FKC049", "FKC050",
            "FKC051", "FKC052", "FKC053", "FKC054", "FKC055", "FKC056", "FKC057", "FKC058", "FKC059", "FKC060",
            "FKC061", "FKC062", "FKC063", "FKC064", "FKC065", "FKC066", "FKC067", "FKC068", "FKC069", "FKC070",
            "FKC071", "FKC072", "FKC073", "FKC074", "FKC075", "FKC076", "FKC077", "FKC078", "FKC079", "FKC080",
            "FKC081", "FKC082", "FKC083", "FKC084", "FKC085", "FKC086", "FKC087", "FKC088", "FKC089", "FKC090",
            "FKC091", "FKC092", "FKC093", "FKC094", "FKC095", "FKC096", "FKC097", "FKC098", "FKC099", "FKC100",
            "FKC101", "FKC102", "FKC103", "FKC104", "FKC105", "FKC106", "FKC107", "FKC108", "FKC109", "FKC110",
            "FKC111", "FKC112", "FKC113", "FKC114", "FKC115", "FKC116", "FKC117", "FKC118", "FKC119", "FKC120",
            
            # Cross Valley Canal - COMPLETE SYSTEM
            "CVC001", "CVC002", "CVC003", "CVC004", "CVC005", "CVC006", "CVC007", "CVC008", "CVC009", "CVC010",
            "CVC011", "CVC012", "CVC013", "CVC014", "CVC015", "CVC016", "CVC017", "CVC018", "CVC019", "CVC020",
            "CVC021", "CVC022", "CVC023", "CVC024", "CVC025", "CVC026", "CVC027", "CVC028", "CVC029", "CVC030",
            "CVC031", "CVC032", "CVC033", "CVC034", "CVC035", "CVC036", "CVC037", "CVC038", "CVC039", "CVC040",
            "CVC041", "CVC042", "CVC043", "CVC044", "CVC045", "CVC046", "CVC047", "CVC048", "CVC049", "CVC050",
            
            # COMPLETE SAN JOAQUIN RIVER SYSTEM - All SJR nodes
            "SJR001", "SJR002", "SJR003", "SJR004", "SJR005", "SJR006", "SJR007", "SJR008", "SJR009", "SJR010",
            "SJR011", "SJR012", "SJR013", "SJR014", "SJR015", "SJR016", "SJR017", "SJR018", "SJR019", "SJR020",
            "SJR021", "SJR022", "SJR023", "SJR024", "SJR025", "SJR026", "SJR027", "SJR028", "SJR029", "SJR030",
            "SJR031", "SJR032", "SJR033", "SJR034", "SJR035", "SJR036", "SJR037", "SJR038", "SJR039", "SJR040",
            "SJR041", "SJR042", "SJR043", "SJR044", "SJR045", "SJR046", "SJR047", "SJR048", "SJR049", "SJR050",
            "SJR051", "SJR052", "SJR053", "SJR054", "SJR055", "SJR056", "SJR057", "SJR058", "SJR059", "SJR060",
            "SJR061", "SJR062", "SJR063", "SJR064", "SJR065", "SJR066", "SJR067", "SJR068", "SJR069", "SJR070",
            "SJRW", "SJRE",
            
            # Key delivery points and infrastructure
            "BAKRSF", "KRNRVR", "TULAR", "PIXLEY", "DELANO", "WASCO", "ANTLP", "BANOS"
        ],
        "key_infrastructure": ["DMC003", "OMR028", "SLUIS", "MILLR", "MENDOTA", "MDOTA", "BAKRSF", "SJRE", "SJRW"],
        "region": "SJR"
    },
    
    "state_water_project_south": {
        "name": "State Water Project - Southern California - MASSIVE COMPREHENSIVE",
        "description": "COMPLETE California Aqueduct to Southern California with ALL 400+ mile infrastructure",
        "nodes": [
            # CRITICAL DELTA PUMPING - Banks Pumping Plant
            "CAA003", "OMR027",  # Banks PP and Delta diversion point
            
            # COMPLETE CALIFORNIA AQUEDUCT MAIN STEM - All CAA nodes
            "CAA000", "CAA001", "CAA002", "CAA004", "CAA005", "CAA006", "CAA007", "CAA008", "CAA009", "CAA010",
            "CAA011", "CAA012", "CAA013", "CAA014", "CAA015", "CAA016", "CAA017", "CAA018", "CAA019", "CAA020",
            "CAA021", "CAA022", "CAA023", "CAA024", "CAA025", "CAA026", "CAA027", "CAA028", "CAA029", "CAA030",
            "CAA031", "CAA032", "CAA033", "CAA034", "CAA035", "CAA036", "CAA037", "CAA038", "CAA039", "CAA040",
            "CAA041", "CAA042", "CAA043", "CAA044", "CAA045", "CAA046", "CAA047", "CAA048", "CAA049", "CAA050",
            "CAA051", "CAA052", "CAA053", "CAA054", "CAA055", "CAA056", "CAA057", "CAA058", "CAA059", "CAA060",
            "CAA061", "CAA062", "CAA063", "CAA064", "CAA065", "CAA066", "CAA067", "CAA068", "CAA069", "CAA070",
            "CAA071", "CAA072", "CAA073", "CAA074", "CAA075", "CAA076", "CAA077", "CAA078", "CAA079", "CAA080",
            "CAA081", "CAA082", "CAA083", "CAA084", "CAA085", "CAA086", "CAA087", "CAA088", "CAA089", "CAA090",
            "CAA091", "CAA092", "CAA093", "CAA094", "CAA095", "CAA096", "CAA097", "CAA098", "CAA099", "CAA100",
            
            # MAJOR SWP RESERVOIRS
            "PYRMD", "CSTLC", "ANTLP", "TEHCP", "PYRAMID", "DVLAKE",
            
            # WEST BRANCH - Los Angeles Aqueduct
            "WBRNCH", "WBA001", "WBA005", "WBA010", "WBA015", "WBA020", "WBA025", "WBA030",
            "C_WBA001", "C_WBA005", "C_WBA010", "C_WBA015", "C_WBA020", "C_WBA025",
            
            # EAST BRANCH - Inland Empire 
            "EBRNCH", "EBA001", "EBA005", "EBA010", "EBA015", "EBA020", "EBA025", "EBA030",
            "C_EBA001", "C_EBA005", "C_EBA010", "C_EBA015", "C_EBA020", "C_EBA025",
            "MOJAVE", "SLVRK", "PEARBM", "PRRIS",
            
            # SAN DIEGO CONNECTION - Complete branch
            "DMOND", "OLIVH", "SNVCNT", "SDA001", "SDA005", "SDA010", "SDA015", "SDA020"
        ],
        "key_infrastructure": ["CAA003", "OMR027", "PYRMD", "CSTLC", "ANTLP", "TEHCP", "PYRAMID", "DVLAKE"],
        "region": "SCLA"
    },
    
    "kern_river_system": {
        "name": "Kern River and Tulare Basin System - MASSIVE COMPREHENSIVE", 
        "description": "COMPLETE Kern River water supply and Tulare Basin infrastructure with ALL tributaries",
        "nodes": [
            # COMPLETE KERN RIVER MAIN STEM - All KRN nodes
            "ISBLK", "SUCCS", "KRNRVR", "KRN001", "KRN002", "KRN003", "KRN004", "KRN005", "KRN006", "KRN007", "KRN008", "KRN009", "KRN010",
            "KRN011", "KRN012", "KRN013", "KRN014", "KRN015", "KRN016", "KRN017", "KRN018", "KRN019", "KRN020",
            "KRN021", "KRN022", "KRN023", "KRN024", "KRN025", "KRN026", "KRN027", "KRN028", "KRN029", "KRN030",
            "KRN031", "KRN032", "KRN033", "KRN034", "KRN035", "KRN036", "KRN037", "KRN038", "KRN039", "KRN040",
            "KRN041", "KRN042", "KRN043", "KRN044", "KRN045", "KRN046", "KRN047", "KRN048", "KRN049", "KRN050",
            "KRN051", "KRN052", "KRN053", "KRN054", "KRN055", "KRN056", "KRN057", "KRN058", "KRN059", "KRN060",
            "KRN061", "KRN062", "KRN063", "KRN064", "KRN065", "KRN066", "KRN067", "KRN068", "KRN069", "KRN070",
            "KRN071", "KRN072", "KRN073", "KRN074", "KRN075", "KRN076", "KRN077", "KRN078", "KRN079", "KRN080",
            "KRN081", "KRN082", "KRN083", "KRN084", "KRN085", "KRN086", "KRN087", "KRN088", "KRN089", "KRN090",
            "C_KRN001", "C_KRN005", "C_KRN010", "C_KRN015", "C_KRN020", "C_KRN025", "C_KRN030", "C_KRN035", "C_KRN040",
            
            # COMPLETE TULARE BASIN - All TLB nodes
            "TULAR", "TLB001", "TLB002", "TLB003", "TLB004", "TLB005", "TLB006", "TLB007", "TLB008", "TLB009", "TLB010",
            "TLB011", "TLB012", "TLB013", "TLB014", "TLB015", "TLB016", "TLB017", "TLB018", "TLB019", "TLB020",
            "TLB021", "TLB022", "TLB023", "TLB024", "TLB025", "TLB026", "TLB027", "TLB028", "TLB029", "TLB030",
            "TLB031", "TLB032", "TLB033", "TLB034", "TLB035", "TLB036", "TLB037", "TLB038", "TLB039", "TLB040",
            "C_TLB001", "C_TLB005", "C_TLB010", "C_TLB015", "C_TLB020", "C_TLB025", "C_TLB030",
            "PIXLEY", "DELANO", "WASCO", "BAKRSF",
            
            # COMPLETE KINGS RIVER SYSTEM - All KGS nodes and reservoirs
            "PINFT", "KGSRVR", "KGS001", "KGS002", "KGS003", "KGS004", "KGS005", "KGS006", "KGS007", "KGS008", "KGS009", "KGS010",
            "KGS011", "KGS012", "KGS013", "KGS014", "KGS015", "KGS016", "KGS017", "KGS018", "KGS019", "KGS020",
            "KGS021", "KGS022", "KGS023", "KGS024", "KGS025", "KGS026", "KGS027", "KGS028", "KGS029", "KGS030",
            "KGS031", "KGS032", "KGS033", "KGS034", "KGS035", "KGS036", "KGS037", "KGS038", "KGS039", "KGS040",
            "C_KGS001", "C_KGS005", "C_KGS010", "C_KGS015", "C_KGS020", "C_KGS025", "C_KGS030",
            
            # TULARE BASIN RESERVOIRS
            "MDSTO", "CAPLS", "BTVLY", "HNSLY"
        ],
        "key_infrastructure": ["ISBLK", "SUCCS", "KRNRVR", "TULAR", "PINFT", "BAKRSF", "PIXLEY"],
        "region": "TLB"
    },
    
    "colorado_river_system": {
        "name": "Colorado River Aqueduct System - MASSIVE COMPREHENSIVE",
        "description": "COMPLETE Colorado River water to Southern California with ALL 240+ mile infrastructure",
        "nodes": [
            # COMPLETE COLORADO RIVER AQUEDUCT - All CRA nodes
            "HAVASU", "PARKE", "IRONMT", "EAGLMT", "GENMTN", "HINDS", "COPLND", "ALAMTN", "CSTLRC", "LKMAT", "SKINNR", "DVLAKE",
            "CRA001", "CRA002", "CRA003", "CRA004", "CRA005", "CRA006", "CRA007", "CRA008", "CRA009", "CRA010",
            "CRA011", "CRA012", "CRA013", "CRA014", "CRA015", "CRA016", "CRA017", "CRA018", "CRA019", "CRA020",
            "CRA021", "CRA022", "CRA023", "CRA024", "CRA025", "CRA026", "CRA027", "CRA028", "CRA029", "CRA030",
            "CRA031", "CRA032", "CRA033", "CRA034", "CRA035", "CRA036", "CRA037", "CRA038", "CRA039", "CRA040",
            "C_CRA001", "C_CRA005", "C_CRA010", "C_CRA015", "C_CRA020", "C_CRA025", "C_CRA030",
            
            # COMPLETE ALL-AMERICAN CANAL - All AAC nodes
            "IMPRVL", "ALLAM", "AAC001", "AAC002", "AAC003", "AAC004", "AAC005", "AAC006", "AAC007", "AAC008", "AAC009", "AAC010",
            "AAC011", "AAC012", "AAC013", "AAC014", "AAC015", "AAC016", "AAC017", "AAC018", "AAC019", "AAC020",
            "AAC021", "AAC022", "AAC023", "AAC024", "AAC025", "AAC026", "AAC027", "AAC028", "AAC029", "AAC030",
            "AAC031", "AAC032", "AAC033", "AAC034", "AAC035", "AAC036", "AAC037", "AAC038", "AAC039", "AAC040",
            "AAC041", "AAC042", "AAC043", "AAC044", "AAC045", "AAC046", "AAC047", "AAC048", "AAC049", "AAC050",
            "AAC051", "AAC052", "AAC053", "AAC054", "AAC055", "AAC056", "AAC057", "AAC058", "AAC059", "AAC060",
            "AAC061", "AAC062", "AAC063", "AAC064", "AAC065", "AAC066", "AAC067", "AAC068", "AAC069", "AAC070",
            "AAC071", "AAC072", "AAC073", "AAC074", "AAC075", "AAC076", "AAC077", "AAC078", "AAC079", "AAC080",
            "C_AAC001", "C_AAC005", "C_AAC010", "C_AAC015", "C_AAC020", "C_AAC025", "C_AAC030",
            
            # COMPLETE COACHELLA CANAL - All CCH nodes
            "COACHLL", "CCH001", "CCH002", "CCH003", "CCH004", "CCH005", "CCH006", "CCH007", "CCH008", "CCH009", "CCH010",
            "CCH011", "CCH012", "CCH013", "CCH014", "CCH015", "CCH016", "CCH017", "CCH018", "CCH019", "CCH020",
            "CCH021", "CCH022", "CCH023", "CCH024", "CCH025", "CCH026", "CCH027", "CCH028", "CCH029", "CCH030",
            "CCH031", "CCH032", "CCH033", "CCH034", "CCH035", "CCH036", "CCH037", "CCH038", "CCH039", "CCH040",
            "C_CCH001", "C_CCH005", "C_CCH010", "C_CCH015", "C_CCH020", "C_CCH025", "C_CCH030"
        ],
        "key_infrastructure": ["HAVASU", "PARKE", "IMPRVL", "DVLAKE", "COACHLL", "IRONMT", "EAGLMT"],
        "region": "COLO"
    },
    
    "san_joaquin_comprehensive": {
        "name": "San Joaquin River Comprehensive System - ABSOLUTELY MASSIVE",
        "description": "COMPLETE San Joaquin River basin with ALL tributaries, reservoirs, and infrastructure",
        "nodes": [
            # COMPLETE MAIN SAN JOAQUIN RIVER - All SJR nodes
            "SJR001", "SJR002", "SJR003", "SJR004", "SJR005", "SJR006", "SJR007", "SJR008", "SJR009", "SJR010",
            "SJR011", "SJR012", "SJR013", "SJR014", "SJR015", "SJR016", "SJR017", "SJR018", "SJR019", "SJR020",
            "SJR021", "SJR022", "SJR023", "SJR024", "SJR025", "SJR026", "SJR027", "SJR028", "SJR029", "SJR030",
            "SJR031", "SJR032", "SJR033", "SJR034", "SJR035", "SJR036", "SJR037", "SJR038", "SJR039", "SJR040",
            "SJR041", "SJR042", "SJR043", "SJR044", "SJR045", "SJR046", "SJR047", "SJR048", "SJR049", "SJR050",
            "SJR051", "SJR052", "SJR053", "SJR054", "SJR055", "SJR056", "SJR057", "SJR058", "SJR059", "SJR060",
            "SJR061", "SJR062", "SJR063", "SJR064", "SJR065", "SJR066", "SJR067", "SJR068", "SJR069", "SJR070",
            "C_SJR001", "C_SJR005", "C_SJR009", "C_SJR013", "C_SJR022", "C_SJR026", "C_SJR028", "C_SJR033",
            "C_SJR038", "C_SJR042", "C_SJR048", "C_SJR053", "C_SJR056", "C_SJR062", "C_SJR070",
            "SJRW", "SJRE",
            
            # COMPLETE STANISLAUS RIVER SYSTEM - All STS nodes and reservoirs
            "DONPD", "NMLNS", "TULLK", "BEARD", "STS001", "STS002", "STS003", "STS004", "STS005", "STS006", "STS007", "STS008", "STS009", "STS010",
            "STS011", "STS012", "STS013", "STS014", "STS015", "STS016", "STS017", "STS018", "STS019", "STS020",
            "STS021", "STS022", "STS023", "STS024", "STS025", "STS026", "STS027", "STS028", "STS029", "STS030",
            "STS031", "STS032", "STS033", "STS034", "STS035", "STS036", "STS037", "STS038", "STS039", "STS040",
            "STS041", "STS042", "STS043", "STS044", "STS045", "STS046", "STS047", "STS048", "STS049", "STS050",
            "C_STS001", "C_STS005", "C_STS010", "C_STS015", "C_STS020", "C_STS025", "C_STS030",
            
            # COMPLETE TUOLUMNE RIVER SYSTEM - All TUO nodes and reservoirs
            "HETCH", "DMPDN", "EXCQR", "TULLK", "MODTO", "PEDRO", "TUO001", "TUO002", "TUO003", "TUO004", "TUO005", "TUO006", "TUO007", "TUO008", "TUO009", "TUO010",
            "TUO011", "TUO012", "TUO013", "TUO014", "TUO015", "TUO016", "TUO017", "TUO018", "TUO019", "TUO020",
            "TUO021", "TUO022", "TUO023", "TUO024", "TUO025", "TUO026", "TUO027", "TUO028", "TUO029", "TUO030",
            "TUO031", "TUO032", "TUO033", "TUO034", "TUO035", "TUO036", "TUO037", "TUO038", "TUO039", "TUO040",
            "TUO041", "TUO042", "TUO043", "TUO044", "TUO045", "TUO046", "TUO047", "TUO048", "TUO049", "TUO050",
            "TUO051", "TUO052", "TUO053", "TUO054", "TUO055", "TUO056", "TUO057", "TUO058", "TUO059", "TUO060",
            "TUO061", "TUO062", "TUO063", "TUO064", "TUO065", "TUO066", "TUO067", "TUO068", "TUO069", "TUO070",
            "C_TUO001", "C_TUO005", "C_TUO009", "C_TUO015", "C_TUO020", "C_TUO025", "C_TUO030",
            
            # COMPLETE MERCED RIVER SYSTEM - All MCD nodes and reservoirs
            "MCCLR", "EXCQR", "MCLRE", "MELON", "MCD001", "MCD002", "MCD003", "MCD004", "MCD005", "MCD006", "MCD007", "MCD008", "MCD009", "MCD010",
            "MCD011", "MCD012", "MCD013", "MCD014", "MCD015", "MCD016", "MCD017", "MCD018", "MCD019", "MCD020",
            "MCD021", "MCD022", "MCD023", "MCD024", "MCD025", "MCD026", "MCD027", "MCD028", "MCD029", "MCD030",
            "MCD031", "MCD032", "MCD033", "MCD034", "MCD035", "MCD036", "MCD037", "MCD038", "MCD039", "MCD040",
            "MCD041", "MCD042", "MCD043", "MCD044", "MCD045", "MCD046", "MCD047", "MCD048", "MCD049", "MCD050",
            "MCD051", "MCD052", "MCD053", "MCD054", "MCD055", "MCD056", "C_MCD001", "C_MCD005", "C_MCD009",
            
            # ALL SAN JOAQUIN VALLEY TREATMENT PLANTS
            "WTPMOD", "WTPJJO", "WTPCOL", "WTPMNR", "WTPDEF", "WTPTAB", "WTPBLV"
        ],
        "key_infrastructure": ["HETCH", "DONPD", "MCCLR", "PINFT", "PEDRO", "SJRE", "SJRW", "MODTO", "TULLK"],
        "region": "SJR"
    },
    
    "delta_pumping_system": {
        "name": "Delta Pumping System - Banks and Jones",
        "description": "Critical Delta export pumping infrastructure for State Water Project and Central Valley Project",
        "nodes": [
            # Delta diversion and pumping infrastructure
            "OMR027", "OMR028",  # Old/Middle River diversion points
            "CAA003",  # Harvey O. Banks Pumping Plant (State Water Project)
            "DMC003",  # C.W. "Bill" Jones Pumping Plant (Central Valley Project)
            # Immediate downstream connections
            "CAA000", "CAA005", "DMC007", "DMC000",
            # Delta outflow and inflow points
            "SJRW", "SJRE", "SAC000"
        ],
        "key_infrastructure": ["CAA003", "DMC003", "OMR027", "OMR028"],
        "region": "DELTA"
    }
}

# MASSIVE INFRASTRUCTURE LIST - Include hundreds more nodes for rich trails
KEY_INFRASTRUCTURE_CODES = [
    # ALL 68 MAJOR RESERVOIRS
    "SHSTA", "OROVL", "FOLSM", "SLUIS", "HETCH", "TRNTY", "WKYTN", "KSWCK", "LWSTN", "NTOMA",
    "AMADR", "MILLR", "DONPD", "MCCLR", "ENGLB", "CMPFW", "ISBLK", "SUCCS", "PINFT", "PYRMD",
    "CSTLC", "ANTLP", "TEHCP", "HAVASU", "PARKE", "IMPRVL", "DVLAKE", "COACHLL", "ALMNR", "BANOS",
    "BLKBT", "BOWMN", "BRYSA", "BTVLY", "CAPLS", "CLRLK", "CMBIE", "CMCHE", "DAVIS", "ENGLB",
    "EPARK", "ESTMN", "FRDYC", "FRMAN", "FRMDW", "HHOLE", "HNSLY", "ICEHS", "INDVL", "JKSMD",
    "JNKSN", "LBEAR", "LGRSV", "LKVLY", "LOONL", "LOSVQ", "MCLRE", "MDSTO", "MELON", "MERLC",
    "MLRTN", "MNRRH", "MTMDW", "NBLDB", "NHGAN", "PARDE", "PEDRO", "RLLNS", "RVPHB", "SCOTF",
    "SGRGE", "SILVR", "SLTSP", "SLYCK", "SPLDG", "STMMD", "TBAUD", "THRMA", "THRMF", "TRLCH",
    "TULOC", "UNVLY", "WDWRD",
    
    # ALL TREATMENT PLANTS (39 WTPs + 22 WWTPs)
    "WTPAMC", "WTPAUB", "WTPBJM", "WTPBLV", "WTPBNC", "WTPBTB", "WTPBUK", "WTPBWM", "WTPCMT",
    "WTPCOL", "WTPCSD", "WTPDEF", "WTPDGT", "WTPDWP", "WTPDWS", "WTPEDH", "WTPELD", "WTPFBN",
    "WTPFMH", "WTPFOL", "WTPFSS", "WTPFTH", "WTPJAC", "WTPJJO", "WTPJMS", "WTPJYL", "WTPMNR",
    "WTPMOD", "WTPNBR", "WTPOPH", "WTPRSV", "WTPSAC", "WTPSJP", "WTPSRW", "WTPTAB", "WTPVNY",
    "WTPWAL", "WTPWDH", "WTPWMN", "AWWWTP", "CCWWTP", "CHWWTP", "DCWWTP", "DVWWTP", "EAWWTP",
    "EDWWTP", "LCWWTP", "MAWWTP", "MDWWTP", "MOWWTP", "ORWWTP", "PGWWTP", "SRWWTP", "STWWTP",
    "SWWWTP", "TCWWTP", "TKWWTP", "WLWWTP", "WSWWTP", "WTPCYC", "YCWWTP",
    
    # ALL PUMP STATIONS
    "DWRPS1", "DWRPS2", "RD1500",
    
    # MAJOR RIVER JUNCTIONS AND ENDPOINTS
    "SAC000", "SAC043", "SAC083", "SJRE", "SJRW", "MDOTA", "MENDOTA", "MODTO", "TULLK",
    "KRNRVR", "TULAR", "BAKRSF", "PIXLEY", "DELANO", "WASCO", "KGSRVR", "PYRAMID", "MOJAVE", "SLVRK", "PEARBM",
    
    # KEY PUMP STATIONS AND DIVERSIONS
    "CALA", "SLUISC", "DMPDN", "EXCQR", "QUNTO", "WBRNCH", "EBRNCH", "DMOND", "OLIVH", "SNVCNT",
    "IRONMT", "EAGLMT", "GENMTN", "HINDS", "COPLND", "ALAMTN",
    
    # CRITICAL DELTA PUMPING STATIONS
    "CAA003", "DMC003", "OMR027", "OMR028"  # Banks PP, Jones PP, Delta diversions
]


async def get_water_trail_from_reservoir(
    db_pool: asyncpg.Pool,
    reservoir_short_code: str,
    trail_type: str = "infrastructure",
    max_depth: int = 6
) -> Dict[str, Any]:
    """
    Get a comprehensive water trail from a major reservoir using hybrid approach:
    1. Start with hardcoded backbone pathway (if available)
    2. Dynamically expand to include ALL connected infrastructure
    This ensures complete connectivity while maintaining curated major pathways
    """
    
    # Find which trail this reservoir belongs to
    trail_data = None
    for trail_key, trail_info in CALIFORNIA_WATER_TRAILS.items():
        if reservoir_short_code in trail_info["nodes"]:
            trail_data = trail_info.copy()  # Make a copy to avoid modifying original
            trail_data["trail_key"] = trail_key
            break
    
    if not trail_data:
        # Fallback to nearby infrastructure
        trail_nodes = await _get_nearby_infrastructure(db_pool, reservoir_short_code, max_depth)
        trail_data = {
            "name": f"Infrastructure near {reservoir_short_code}",
            "description": "Nearby water infrastructure",
            "nodes": trail_nodes,
            "key_infrastructure": [reservoir_short_code],
            "region": "UNKNOWN",
            "trail_key": "fallback"
        }
    
    # HYBRID APPROACH: Expand hardcoded trails with all connected infrastructure
    # This addresses the sparseness issue by including tributaries, diversions, etc.
    if trail_data["trail_key"] != "fallback":
        print(f"🔄 Expanding {trail_data['name']} with connected infrastructure...")
        expanded_nodes = await _expand_trail_with_connected_infrastructure(
            db_pool, 
            trail_data["nodes"], 
            reservoir_short_code,
            max_depth=4  # Reasonable depth for connected infrastructure
        )
        
        # Combine hardcoded backbone with expanded infrastructure
        all_trail_nodes = list(set(trail_data["nodes"] + expanded_nodes))
        trail_data["nodes"] = all_trail_nodes
        trail_data["expansion_added"] = len(expanded_nodes)
        trail_data["total_nodes"] = len(all_trail_nodes)
        
        print(f"✅ Trail expansion: {len(trail_data['nodes']) - len(expanded_nodes)} backbone + {len(expanded_nodes)} connected = {len(all_trail_nodes)} total")
    
    # Get trail elements with geometry
    trail_features = await _get_trail_geojson(db_pool, trail_data["nodes"])
    
    # Deduplicate features by short_code (in case hardcoded trails have duplicates)
    seen_codes = set()
    deduplicated_features = []
    
    for feature in trail_features:
        short_code = feature["properties"]["short_code"]
        if short_code not in seen_codes:
            seen_codes.add(short_code)
            deduplicated_features.append(feature)
    
    if len(deduplicated_features) < len(trail_features):
        print(f"Individual trail deduplication: {len(trail_features)} → {len(deduplicated_features)} features")
        trail_features = deduplicated_features
    
    # Include ALL connectivity for complete trail visualization
    # No filtering - show the complete water pathway for each system
    # This will create proper connected trails instead of sparse nodes
    
    # Build comprehensive metadata including expansion info
    metadata = {
        "start_reservoir": reservoir_short_code,
        "trail_name": trail_data["name"],
        "trail_description": trail_data["description"],
        "trail_type": trail_type,
        "trail_key": trail_data["trail_key"],
        "region": trail_data["region"],
        "total_features": len(trail_features),
        "full_trail_size": len(trail_data["nodes"]),
        "approach": "hybrid_hardcoded_plus_expansion" if trail_data["trail_key"] != "fallback" else "dynamic_connectivity",
        "foundation": "network_topology_comprehensive_connectivity"
    }
    
    # Add expansion statistics if this was an expanded trail
    if "expansion_added" in trail_data:
        metadata["expansion_added"] = trail_data["expansion_added"]
        metadata["total_nodes"] = trail_data["total_nodes"]
        metadata["backbone_nodes"] = trail_data["total_nodes"] - trail_data["expansion_added"]
        metadata["expansion_ratio"] = round(trail_data["expansion_added"] / trail_data["total_nodes"], 2) if trail_data["total_nodes"] > 0 else 0
    
    return {
        "type": "FeatureCollection",
        "features": trail_features,
        "metadata": metadata
    }


async def get_major_reservoir_trails(
    db_pool: asyncpg.Pool,
    trail_type: str = "infrastructure"
) -> Dict[str, Any]:
    """
    Get overview of all major California water trails
    Shows the big picture of California water system
    """
    
    all_features = []
    trail_summaries = []
    
    # Get top 9 reservoirs by capacity + major system reservoirs
    top_reservoirs = await _get_top_9_reservoirs(db_pool)
    major_system_reservoirs = ["SHSTA", "OROVL", "FOLSM", "SLUIS", "HETCH", "TRNTY", "WKYTN", "AMADR", "MILLR"]
    
    # Combine and deduplicate
    all_major_reservoirs = list(set(top_reservoirs + major_system_reservoirs))
    
    for reservoir in all_major_reservoirs:
        try:
            # Use expanded trail approach for comprehensive connectivity
            trail_data = await get_water_trail_from_reservoir(
                db_pool, reservoir, trail_type, max_depth=5  # Slightly higher for overview
            )
            
            # Add trail system metadata to features
            for feature in trail_data["features"]:
                feature["properties"]["trail_system"] = trail_data["metadata"]["trail_key"]
                feature["properties"]["trail_name"] = trail_data["metadata"]["trail_name"]
                feature["properties"]["region"] = trail_data["metadata"]["region"]
                
                # Mark expanded infrastructure for frontend styling
                if "expansion_added" in trail_data["metadata"]:
                    feature["properties"]["is_expanded_infrastructure"] = True
            
            all_features.extend(trail_data["features"])
            
            # Enhanced trail summary with expansion info
            summary = {
                "reservoir": reservoir,
                "trail_key": trail_data["metadata"]["trail_key"],
                "trail_name": trail_data["metadata"]["trail_name"],
                "region": trail_data["metadata"]["region"],
                "features": len(trail_data["features"])
            }
            
            # Add expansion statistics if available
            if "expansion_added" in trail_data["metadata"]:
                summary["backbone_nodes"] = trail_data["metadata"]["total_nodes"] - trail_data["metadata"]["expansion_added"]
                summary["expanded_nodes"] = trail_data["metadata"]["expansion_added"]
                summary["expansion_ratio"] = round(trail_data["metadata"]["expansion_added"] / trail_data["metadata"]["total_nodes"], 2)
            
            trail_summaries.append(summary)
            
            print(f"✅ {reservoir}: {len(trail_data['features'])} features" + 
                  (f" (expanded: +{trail_data['metadata'].get('expansion_added', 0)})" if "expansion_added" in trail_data["metadata"] else ""))
            
        except Exception as e:
            print(f"❌ Error getting expanded trail for {reservoir}: {e}")
            continue
    
    # CRITICAL: Deduplicate features by short_code before returning
    # Multiple trail systems can include the same infrastructure (e.g., KSWCK, SAC083)
    # This was causing React key errors in the frontend
    seen_codes = set()
    deduplicated_features = []
    
    for feature in all_features:
        short_code = feature["properties"]["short_code"]
        if short_code not in seen_codes:
            seen_codes.add(short_code)
            deduplicated_features.append(feature)
        else:
            print(f"Deduplicating {short_code} - appears in multiple trail systems")
    
    print(f"Deduplication: {len(all_features)} → {len(deduplicated_features)} features")
    all_features = deduplicated_features
    
    # If we don't have enough features after deduplication, add more key infrastructure
    if len(all_features) < 50:
        print(f"Adding more infrastructure - only have {len(all_features)} features")
        additional_features = await _get_additional_key_infrastructure(db_pool)
        
        # Avoid duplicates (check against already deduplicated features)
        existing_codes = {f["properties"]["short_code"] for f in all_features}
        new_features = [f for f in additional_features if f["properties"]["short_code"] not in existing_codes]
        
        all_features.extend(new_features)
        print(f"Added {len(new_features)} additional infrastructure features")
    
    return {
        "type": "FeatureCollection",
        "features": all_features,
        "metadata": {
            "trail_type": trail_type,
            "total_features": len(all_features),
            "trail_systems": len(trail_summaries),
            "trail_summaries": trail_summaries,
            "approach": "california_water_system_overview",
            "foundation": "hardcoded_major_pathways_with_fallback"
        }
    }


async def _get_top_9_reservoirs(db_pool: asyncpg.Pool) -> List[str]:
    """Get top 9 reservoirs by capacity"""
    
    query = """
    SELECT nt.short_code
    FROM network_topology nt
    LEFT JOIN reservoir_entity re ON nt.short_code = re.short_code
    WHERE nt.type = 'STR'
    AND nt.is_active = true
    AND re.capacity_taf IS NOT NULL
    ORDER BY re.capacity_taf DESC
    LIMIT 9;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    return [row["short_code"] for row in rows]


async def _get_additional_key_infrastructure(
    db_pool: asyncpg.Pool
) -> List[Dict[str, Any]]:
    """Get additional key infrastructure when hardcoded trails are too sparse"""
    
    query = """
    SELECT 
        nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
        nt.from_node, nt.to_node, nt.river_name, nt.arc_name,
        nt.hydrologic_region,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.is_active = true
    AND (
        nt.type IN ('STR', 'PS', 'WTP', 'WWTP') OR
        (nt.type = 'CH' AND nt.river_name IN ('Sacramento River', 'San Joaquin River', 'American River', 'Feather River')) OR
        nt.short_code IN ('SAC000', 'SAC043', 'SAC083', 'SJRE', 'SJRW', 'MDOTA')
    )
    ORDER BY 
        CASE nt.type 
            WHEN 'STR' THEN 1 
            WHEN 'PS' THEN 2 
            WHEN 'WTP' THEN 3 
            WHEN 'WWTP' THEN 4 
            ELSE 5 
        END,
        nt.short_code
    LIMIT 100;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
    
    features = []
    for row in rows:
        try:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            if not geometry:
                continue
            
            properties = {
                'id': row['id'],
                'short_code': row['short_code'],
                'type': row['schematic_type'],
                'element_type': row['type'],
                'subtype': row['sub_type'],
                'connectivity_status': 'connected',
                'trail_element': True,
                'hydrologic_region': row['hydrologic_region'],
                'is_additional_infrastructure': True
            }
            
            if row['schematic_type'] == 'node':
                properties.update({
                    'river_name': row['river_name'],
                    'display_name': row['river_name'] or row['short_code'],
                    'infrastructure_type': _get_infrastructure_type(row['type']),
                    'is_key_infrastructure': row['short_code'] in KEY_INFRASTRUCTURE_CODES
                })
            elif row['schematic_type'] == 'arc':
                properties.update({
                    'arc_name': row['arc_name'],
                    'from_node': row['from_node'],
                    'to_node': row['to_node'],
                    'display_name': row['arc_name'] or f"{row['from_node']} → {row['to_node']}"
                })
            
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing additional infrastructure {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return features


async def _expand_trail_with_connected_infrastructure(
    db_pool: asyncpg.Pool,
    backbone_nodes: List[str],
    primary_reservoir: str,
    max_depth: int = 4
) -> List[str]:
    """
    Expand hardcoded trail backbone with ALL connected infrastructure
    This finds tributaries, diversions, pump stations, treatment plants, etc.
    that connect to the main trail pathway
    """
    
    if not backbone_nodes:
        return []
    
    # Build parameterized query for all backbone nodes
    placeholders = ','.join(f'${i+1}' for i in range(len(backbone_nodes)))
    
    query = f"""
    WITH RECURSIVE connected_infrastructure AS (
        -- Start from all backbone nodes
        SELECT nt.short_code, 0 as depth, 'backbone' as source_type
        FROM network_topology nt 
        WHERE nt.short_code IN ({placeholders})
        AND nt.is_active = true
        
        UNION ALL
        
        -- Find all infrastructure connected to backbone or previously found nodes
        SELECT nt.short_code, ci.depth + 1, 
               CASE 
                   WHEN nt.type IN ('STR', 'PS', 'WTP', 'WWTP') THEN 'key_infrastructure'
                   WHEN nt.type = 'D' THEN 'delivery'
                   WHEN nt.type = 'CH' AND nt.river_name IS NOT NULL THEN 'named_channel'
                   ELSE 'other_infrastructure'
               END as source_type
        FROM connected_infrastructure ci
        JOIN network_topology nt ON (
            nt.from_node = ci.short_code OR nt.to_node = ci.short_code OR
            ci.short_code = nt.from_node OR ci.short_code = nt.to_node
        )
        WHERE ci.depth < $1
        AND nt.is_active = true
        AND nt.short_code NOT IN (
            SELECT short_code FROM connected_infrastructure
        )
        -- Include all infrastructure types for comprehensive trails
        AND nt.type IN ('STR', 'PS', 'WTP', 'WWTP', 'CH', 'D', 'OM', 'NP', 'PR', 'RFS')
    )
    SELECT DISTINCT short_code, source_type
    FROM connected_infrastructure 
    WHERE source_type != 'backbone'  -- Don't return backbone nodes we already have
    ORDER BY 
        CASE source_type
            WHEN 'key_infrastructure' THEN 1
            WHEN 'delivery' THEN 2  
            WHEN 'named_channel' THEN 3
            ELSE 4
        END,
        short_code;
    """
    
    # Add max_depth as the first parameter, then all backbone nodes
    params = [max_depth] + backbone_nodes
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    
    expanded_nodes = [row["short_code"] for row in rows]
    
    # Log expansion details for debugging
    infrastructure_counts = {}
    for row in rows:
        source_type = row["source_type"]
        infrastructure_counts[source_type] = infrastructure_counts.get(source_type, 0) + 1
    
    print(f"🔍 Infrastructure expansion from {primary_reservoir}:")
    for infra_type, count in infrastructure_counts.items():
        print(f"  • {infra_type}: {count} elements")
    
    return expanded_nodes


async def _get_nearby_infrastructure(
    db_pool: asyncpg.Pool,
    start_code: str,
    max_depth: int
) -> List[str]:
    """Fallback: get nearby infrastructure if not in hardcoded trails"""
    
    query = """
    WITH RECURSIVE nearby AS (
        SELECT short_code, 0 as depth
        FROM network_topology 
        WHERE short_code = $1 AND is_active = true
        
        UNION ALL
        
        SELECT nt.short_code, n.depth + 1
        FROM nearby n
        JOIN network_topology nt ON (
            nt.from_node = n.short_code OR nt.to_node = n.short_code
        )
        WHERE n.depth < $2 
        AND nt.is_active = true
        AND nt.type IN ('STR', 'PS', 'WTP', 'WWTP', 'CH')
    )
    SELECT DISTINCT short_code FROM nearby ORDER BY short_code;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, start_code, max_depth)
    
    return [row["short_code"] for row in rows]


async def _get_trail_geojson(
    db_pool: asyncpg.Pool,
    trail_codes: List[str]
) -> List[Dict[str, Any]]:
    """Convert trail node codes to GeoJSON features with geometry"""
    
    if not trail_codes:
        return []
    
    # Build parameterized query
    placeholders = ','.join(f'${i+1}' for i in range(len(trail_codes)))
    
    query = f"""
    SELECT 
        nt.id, nt.short_code, nt.schematic_type, nt.type, nt.sub_type,
        nt.from_node, nt.to_node, nt.river_name, nt.arc_name,
        nt.hydrologic_region,
        ST_AsGeoJSON(ng.geom) as geometry,
        ng.geometry_type
    FROM network_topology nt
    LEFT JOIN network_gis ng ON nt.short_code = ng.short_code
    WHERE nt.short_code IN ({placeholders})
    AND nt.is_active = true
    ORDER BY nt.schematic_type, nt.type, nt.short_code;
    """
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *trail_codes)
    
    features = []
    for row in rows:
        try:
            geometry = json.loads(row['geometry']) if row['geometry'] else None
            if not geometry:
                continue
            
            properties = {
                'id': row['id'],
                'short_code': row['short_code'],
                'type': row['schematic_type'],
                'element_type': row['type'],
                'subtype': row['sub_type'],
                'connectivity_status': 'connected',
                'trail_element': True,
                'hydrologic_region': row['hydrologic_region']
            }
            
            if row['schematic_type'] == 'node':
                properties.update({
                    'river_name': row['river_name'],
                    'display_name': row['river_name'] or row['short_code'],
                    'infrastructure_type': _get_infrastructure_type(row['type']),
                    'is_key_infrastructure': row['short_code'] in KEY_INFRASTRUCTURE_CODES
                })
            elif row['schematic_type'] == 'arc':
                properties.update({
                    'arc_name': row['arc_name'],
                    'from_node': row['from_node'],
                    'to_node': row['to_node'],
                    'display_name': row['arc_name'] or f"{row['from_node']} → {row['to_node']}",
                    'is_key_connection': (row['from_node'] in KEY_INFRASTRUCTURE_CODES or 
                                        row['to_node'] in KEY_INFRASTRUCTURE_CODES)
                })
            
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties
            })
            
        except Exception as e:
            print(f"Error processing trail element {row.get('short_code', 'unknown')}: {e}")
            continue
    
    return features


def _get_infrastructure_type(element_type: str) -> str:
    """Get human-readable infrastructure type"""
    type_mapping = {
        'STR': 'Major Reservoir',
        'PS': 'Pump Station', 
        'WTP': 'Water Treatment Plant',
        'WWTP': 'Wastewater Treatment',
        'CH': 'River Channel',
        'D': 'Water Delivery',
        'OM': 'Water Outlet',
        'NP': 'Water User',
        'PR': 'Water Project'
    }
    return type_mapping.get(element_type, element_type)