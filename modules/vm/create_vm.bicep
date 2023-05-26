// SET MODULE DATE
param module_metadata object = {
  module_last_updated : '2023-05-19'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

param r_usr_mgd_identity_name string

param saName string
param blobContainerName string
param saPrimaryEndpointsBlob string

param logAnalyticsPayGWorkspaceId string

param linDataCollectionEndpointId string
param storeEventsDcrId string
param automationEventsDcrId string

param vmParams object
param vnetName string
param vmName string = replace('${vmParams.vmNamePrefix}-${deploymentParams.enterprise_name_suffix}-${deploymentParams.global_uniqueness}', '_', '-')

param dnsLabelPrefix string = toLower('${vmParams.vmNamePrefix}-${deploymentParams.global_uniqueness}-${uniqueString(resourceGroup().id, vmName)}')
param publicIpName string = '${vmParams.vmNamePrefix}-${deploymentParams.global_uniqueness}-PublicIp'

// var userDataScript = base64(loadTextContent('./bootstrap_scripts/deploy_app.sh'))
var userDataScript = loadFileAsBase64('./bootstrap_scripts/deploy_app.sh')

// @description('VM auth')
// @allowed([
//   'sshPublicKey'
//   'password'
// ])
// param authType string = 'password'

var LinuxConfiguration = {
  disablePasswordAuthentication: true
  ssh: {
    publickeys: [
      {
        path: '/home/${vmParams.adminUsername}/.ssh/authorized_keys'
        keyData: vmParams.adminPassword
      }
    ]
  }
}



// Resource References
resource r_blob_ref 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-04-01' existing = {
  name: '${saName}/default/${blobContainerName}'
}

// Reference existing User-Assigned Identity
resource r_userManagedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: r_usr_mgd_identity_name
}


resource r_publicIp 'Microsoft.Network/publicIPAddresses@2022-05-01' = {
  name: publicIpName
  location: deploymentParams.location
  tags: tags
  sku: {
    name: vmParams.publicIpSku
  }
  properties: {
    publicIPAllocationMethod: vmParams.publicIPAllocationMethod
    publicIPAddressVersion: 'IPv4'
    deleteOption: 'Delete'
    dnsSettings: {
      domainNameLabel: dnsLabelPrefix
    }
  }
}

resource r_webSg 'Microsoft.Network/networkSecurityGroups@2021-05-01' = {
  name: 'webSg'
  location: deploymentParams.location
  tags: tags
  properties: {
    securityRules: [
      {
        name: 'AllowInboundSsh'
        properties: {
          priority: 250
          protocol: 'Tcp'
          access: 'Allow'
          direction: 'Inbound'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '22'
        }
      }
      {
        name: 'HTTP'
        properties: {
          priority: 200
          protocol: 'Tcp'
          access: 'Allow'
          direction: 'Inbound'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '80'
        }
      }
      {
        name: 'Outbound_Allow_All'
        properties: {
          priority: 300
          protocol: '*'
          access: 'Allow'
          direction: 'Outbound'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '*'
        }
      }
      {
        name: 'AzureResourceManager'
        properties: {
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: 'AzureResourceManager'
          access: 'Allow'
          priority: 160
          direction: 'Outbound'
        }
      }
      {
        name: 'AzureStorageAccount'
        properties: {
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: 'Storage.${deploymentParams.location}'
          access: 'Allow'
          priority: 170
          direction: 'Outbound'
        }
      }
      {
        name: 'AzureFrontDoor'
        properties: {
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: 'AzureFrontDoor.FrontEnd'
          access: 'Allow'
          priority: 180
          direction: 'Outbound'
        }
      }
    ]
  }
}

//
resource r_webSgNsgDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  scope: r_webSg
  name: 'default'
  properties: {
    workspaceId: logAnalyticsPayGWorkspaceId
    logs: [
      {
        category: 'NetworkSecurityGroupEvent'
        enabled: true
        retentionPolicy: {
          enabled: false
          days: 0
        }
      }
      {
        category: 'NetworkSecurityGroupRuleCounter'
        enabled: true
        retentionPolicy: {
          enabled: false
          days: 0
        }
      }
    ]
  }
}

// Create NIC for the VM
resource r_nic_01 'Microsoft.Network/networkInterfaces@2022-05-01' = {
  name: '${vmName}-Nic-01'
  location: deploymentParams.location
  tags: tags
  properties: {
    ipConfigurations: [
      {
        name: 'ipconfig1'
        properties: {
          subnet: {
            id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, vmParams.vmSubnetName)
          }
          privateIPAllocationMethod: 'Dynamic'
          publicIPAddress: {
            id: r_publicIp.id
          }
        }
      }
    ]
    networkSecurityGroup: {
      id: r_webSg.id
    }
  }
}




// Add permissions to the custom identity to write to the blob storage
// Azure Built-In Roles Ref: https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles
param blobOwnerRoleId string = 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b'

var blobPermsConditionStr= '((!(ActionMatches{\'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read\'}) AND !(ActionMatches{\'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write\'}) ) OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers:name] StringEquals \'${blobContainerName}\'))'



// Refined Scope with conditions
// https://learn.microsoft.com/en-us/azure/templates/microsoft.authorization/roleassignments?pivots=deployment-language-bicep

resource r_attach_perms_to_role_BlobOwner 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('r_attach_perms_to_role_BlobOwner', r_userManagedIdentity.id, blobOwnerRoleId)
  scope: r_blob_ref
  properties: {
    description: 'Blob Owner Permission to ResourceGroup scope'
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', blobOwnerRoleId)
    principalId: r_userManagedIdentity.properties.principalId
    conditionVersion: '2.0'
    condition: blobPermsConditionStr
    principalType: 'ServicePrincipal'
    // https://learn.microsoft.com/en-us/azure/role-based-access-control/troubleshooting?tabs=bicep#symptom---assigning-a-role-to-a-new-principal-sometimes-fails
  }
}

// Add Monitoring Metrics Publisher Role to the custom identity
param metricsPublisherRoleId string = '3913510d-42f4-4e42-8a64-420c390055eb'

resource r_attach_perms_to_role_MetricsPublisher 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('r_attach_perms_to_role_MetricsPublisher', r_userManagedIdentity.id, metricsPublisherRoleId)
  scope: resourceGroup()
  properties: {
    description: 'Monitoring Metrics Publisher Permission to ResourceGroup scope'
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', metricsPublisherRoleId)
    principalId: r_userManagedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    // https://learn.microsoft.com/en-us/azure/role-based-access-control/troubleshooting?tabs=bicep#symptom---assigning-a-role-to-a-new-principal-sometimes-fails
  }
}


resource r_vm 'Microsoft.Compute/virtualMachines@2022-03-01' = {
  name: vmName
  location: deploymentParams.location
  tags: tags
  identity: {
    // type: 'SystemAssigned'
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_userManagedIdentity.id}': {}
    }
  }
  // zones: [
  //   '3'
  // ]
  properties: {
    hardwareProfile: {
      vmSize: vmParams.vmSize
    }
    osProfile: {
      computerName: vmName
      adminUsername: vmParams.adminUsername
      adminPassword: vmParams.adminPassword.secureString
      linuxConfiguration: ((vmParams.authType == 'password') ? null : LinuxConfiguration)
    }
    storageProfile: {
      imageReference: ((vmParams.isUbuntu == true) ? ({
        publisher: 'Canonical'
        offer: '0001-com-ubuntu-server-jammy'
        sku: '22_04-lts-gen2'
        version: 'latest'
      }) : ({
        publisher: 'RedHat'
        offer: 'RHEL'
        sku: '91-gen2'
        version: 'latest'
      }))
      osDisk: {
        createOption: 'FromImage'
        name: '${vmName}_osDisk'
        caching: 'ReadWrite'
        deleteOption: 'Delete'
        diskSizeGB: 128
        managedDisk: {
          storageAccountType: 'StandardSSD_LRS'
        }
      }
      dataDisks: [
        {
          createOption: 'Empty'
          name: '${vmName}-DataDisk'
          caching: 'ReadWrite'
          deleteOption: 'Delete'
          lun: 13
          diskSizeGB: 2
          managedDisk: {
            storageAccountType: 'StandardSSD_LRS'
            // storageAccountType: 'PremiumV2_LRS' // Apparently needs zones to be defined and AZURE capacity issues - ^.^
          }
        }
      ]
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: r_nic_01.id
        }
      ]
    }
    securityProfile: {
      // encryptionAtHost: true
    }
    diagnosticsProfile: {
      bootDiagnostics: {
        enabled: false
        storageUri: saPrimaryEndpointsBlob
      }
    }
    userData: userDataScript
  }
}

// INSTALL Azure Monitor Agent
resource AzureMonitorLinuxAgent 'Microsoft.Compute/virtualMachines/extensions@2021-07-01' = if (vmParams.isLinux) {
  parent: r_vm
  name: 'AzureMonitorLinuxAgent'
  location: deploymentParams.location
  properties: {
    publisher: 'Microsoft.Azure.Monitor'
    type: 'AzureMonitorLinuxAgent'
    enableAutomaticUpgrade: true
    autoUpgradeMinorVersion: true
    typeHandlerVersion: '1.25'
    settings: {
      'identifier-name': 'mi_res_id' // Has to be this value
      // 'identifier-value': r_vm.identity.principalId
      'identifier-value': r_userManagedIdentity.id
    }
  }
}

// Associate Data Collection Endpoint to VM
// Apparently you cannot name this resource and also it cannot be clubbed with DCR association
resource r_associateDce_To_Vm 'Microsoft.Insights/dataCollectionRuleAssociations@2021-09-01-preview' = {
  name: 'configurationAccessEndpoint'
  scope: r_vm
  properties: {
    dataCollectionEndpointId: linDataCollectionEndpointId
    // dataCollectionRuleId: storeEventsDcrId
    description: 'Send Custom logs to DCR'
  }
}

// Associate Store Events DCR to VM
resource r_associatestoreEventsDcr_To_Vm 'Microsoft.Insights/dataCollectionRuleAssociations@2021-09-01-preview' = {
  name: '${vmName}_storeEventsDcr_${deploymentParams.global_uniqueness}'
  scope: r_vm
  properties: {
    // dataCollectionEndpointId: linDataCollectionEndpointId
    dataCollectionRuleId: storeEventsDcrId
    description: 'Send Application Logs to DCR'
  }
}

// Associate Automation Events DCR to VM
resource r_associateautomationEventsDcr_To_Vm 'Microsoft.Insights/dataCollectionRuleAssociations@2021-09-01-preview' = {
  name: '${vmName}_automationEvents_${deploymentParams.global_uniqueness}'
  scope: r_vm
  properties: {
    // dataCollectionEndpointId: linDataCollectionEndpointId
    dataCollectionRuleId: automationEventsDcrId
    description: 'Send Automation Logs to DCR'
  }
}

resource windowsAgent 'Microsoft.Compute/virtualMachines/extensions@2021-11-01' = if (vmParams.isWindows) {
  name: 'AzureMonitorWindowsAgent'
  parent: r_vm
  location: deploymentParams.location
  properties: {
    publisher: 'Microsoft.Azure.Monitor'
    type: 'AzureMonitorWindowsAgent'
    typeHandlerVersion: '1.0'
    autoUpgradeMinorVersion: true
    enableAutomaticUpgrade: true
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output vmName string = vmName
output webGenHostName string = r_publicIp.properties.dnsSettings.fqdn
output adminUsername string = vmParams.adminUsername
output sshCommand string = 'ssh ${vmParams.adminUsername}@${r_publicIp.properties.dnsSettings.fqdn}'
output webGenHostId string = r_vm.id
output webGenHostPrivateIP string = r_nic_01.properties.ipConfigurations[0].properties.privateIPAddress


