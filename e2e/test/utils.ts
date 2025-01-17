import { Workbench } from 'wdio-vscode-service';


export const wait = async (ms: number): Promise<void> => {
  return await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
};

export const openSnowflakeExtensionView = async (workbench: Workbench) => {
  const viewControl = await workbench.getActivityBar().getViewControl('Snowflake');
  await viewControl?.openView();
};
