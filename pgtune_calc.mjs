import { normalizeConfiguration } from './src/features/configuration/configurationParams.js'
import {
  selectSettings,
  selectWarningInfoMessages,
  selectRestartRequiredSettings
} from './src/features/configuration/configurationSelectors.js'
import { formatSettingValue } from './src/features/configuration/configurationText.js'

const chunks = []
for await (const chunk of process.stdin) chunks.push(chunk)
const input = JSON.parse(Buffer.concat(chunks).toString('utf8'))

const values = {
  dbVersion: String(input.dbVersion),
  osType: input.osType,
  dbType: input.dbType,
  cpuNum: input.cpuNum === null ? '' : String(input.cpuNum),
  totalMemory: String(input.totalMemoryMb),
  totalMemoryUnit: 'MB',
  connectionNum: input.connectionNum === null ? '' : String(input.connectionNum),
  hdType: input.hdType,
  dbSize: input.dbSize
}

const state = { configuration: normalizeConfiguration(values) }

const settings = selectSettings(state).map((setting) => ({
  key: setting.key,
  value: setting.isMemory ? formatSettingValue(setting) : String(setting.value),
  isMemory: Boolean(setting.isMemory)
}))

const warnings = selectWarningInfoMessages(state).map((warning) => ({
  level: warning.level,
  text: warning.text.join(' '),
  code: warning.code || null
}))

const restartRequired = selectRestartRequiredSettings(state)

process.stdout.write(JSON.stringify({ settings, warnings, restartRequired }))