import { Workbench } from "wdio-vscode-service";
import { ExtensionKey } from "./constants";

export const openSnowflakeExtensionView = async (workbench: Workbench) => {
  const viewControl = await workbench.getActivityBar().getViewControl(ExtensionKey);
  await viewControl?.openView();
};

export const wait = async (ms: number): Promise<void> => {
  return await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
};
