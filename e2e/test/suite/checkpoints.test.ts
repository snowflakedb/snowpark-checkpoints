import { after, before, describe, it } from "mocha";
import * as assert from "assert";
import * as h from "../helpers/helpers";

describe("Snowpark Checkpoints Test Suite", () => {
  before(() => {
    h.clearCheckpointsConfiguration();
  });

  after(() => {
    h.clearCheckpointsConfiguration();
  });

  it("should extension be activated", async () => {
    // arrange
    const extension = h.getCheckpointsExtension();
    // act
    await extension.activate();
    // assert
    assert.ok(extension.isActive);
  });

  it("should validate initial default settings", async () => {
    // arrange
    const checkpointsConfiguration = h.getCheckpointsConfiguration();
    const enabled = checkpointsConfiguration.get("enabled");
    const initializationEnabled = checkpointsConfiguration.get("initializationEnabled");
    const actual = { enabled, initializationEnabled };
    const expected = { enabled: false, initializationEnabled: true };
    // assert
    assert.deepStrictEqual(actual, expected);
  });

  it("should enable checkpoints feature", async () => {
    // arrange
    await h.activateCheckpointsExtension();
    const initial = h.isCheckpointsFeatureEnabled();
    // act
    await h.enableCheckpointsFeature();
    const result = h.isCheckpointsFeatureEnabled();
    // assert
    assert.ok(initial === false);
    assert.ok(result);
  });
});
