import * as vscode from "vscode";
import * as assert from "assert";

const ExtensionID = "snowflake.snowflake-vsc";
const ConfigurationSection = "snowflake.snowparkCheckpoints";

export const activateCheckpointsExtension = async (): Promise<void> => {
  const extension = getCheckpointsExtension();
  await extension.activate();
  assert.ok(extension.isActive);
};

export const clearCheckpointsConfiguration = (): void => {
  const configuration = getCheckpointsConfiguration();
  configuration.update("enabled", false, vscode.ConfigurationTarget.Global);
  configuration.update("initializationEnabled", true, vscode.ConfigurationTarget.Global);
};

export const enableCheckpointsFeature = async (): Promise<void> => {
  const configuration = getCheckpointsConfiguration();
  await configuration.update("enabled", true, vscode.ConfigurationTarget.Global);
};

export const isCheckpointsFeatureEnabled = (): boolean => {
  const configuration = getCheckpointsConfiguration();
  return configuration.get("enabled") ?? false;
};

export const getCheckpointsExtension = (): vscode.Extension<any> => {
  const extension = vscode.extensions.getExtension(ExtensionID);
  assert.ok(extension);
  return extension;
};

export const getCheckpointsConfiguration = (): vscode.WorkspaceConfiguration => {
  const configuration = vscode.workspace.getConfiguration(ConfigurationSection);
  assert.ok(configuration);
  return configuration;
};
