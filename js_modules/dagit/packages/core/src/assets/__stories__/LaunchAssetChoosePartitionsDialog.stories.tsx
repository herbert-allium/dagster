import {MockedProvider} from '@apollo/client/testing';
import {Meta} from '@storybook/react/types-6-0';
import * as React from 'react';

import {StorybookProvider} from '../../testing/StorybookProvider';
import {LaunchAssetChoosePartitionsDialog} from '../LaunchAssetChoosePartitionsDialog';
import {ReleasesJobProps} from '../__fixtures__/LaunchAssetChoosePartitionsDialog.fixtures';
import {
  ReleaseFiles,
  ReleaseFilesMetadata,
  ReleaseZips,
  ReleasesMetadata,
  ReleasesSummary,
} from '../__fixtures__/PartitionHealthQuery.fixtures';
import {NoRunningBackfills} from '../__fixtures__/RunningBackfillsNoticeQuery.fixture';

// eslint-disable-next-line import/no-default-export
export default {
  title: 'LaunchAssetChoosePartitionsDialog',
  component: LaunchAssetChoosePartitionsDialog,
} as Meta;

export const Empty = () => {
  return (
    <StorybookProvider>
      <MockedProvider
        mocks={[
          ReleaseFiles(true),
          ReleaseFilesMetadata(true),
          ReleaseZips(true),
          ReleasesMetadata(true),
          ReleasesSummary(true),
        ]}
      >
        <LaunchAssetChoosePartitionsDialog
          {...ReleasesJobProps}
          open={true}
          setOpen={function () {}}
        />
      </MockedProvider>
    </StorybookProvider>
  );
};

export const Ordinal = () => {
  return (
    <StorybookProvider>
      <MockedProvider
        mocks={[
          ReleaseFiles(),
          ReleaseFilesMetadata(),
          ReleaseZips(),
          ReleasesMetadata(),
          ReleasesSummary(),
          NoRunningBackfills,
        ]}
      >
        <LaunchAssetChoosePartitionsDialog
          {...ReleasesJobProps}
          open={true}
          setOpen={function () {}}
        />
      </MockedProvider>
    </StorybookProvider>
  );
};
