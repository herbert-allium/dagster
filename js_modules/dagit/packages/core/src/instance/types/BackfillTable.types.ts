// Generated GraphQL types, do not edit manually.

import * as Types from '../../graphql/types';

export type BackfillTableFragment = {
  __typename: 'PartitionBackfill';
  backfillId: string;
  status: Types.BulkActionStatus;
  numCancelable: number;
  partitionNames: Array<string> | null;
  isValidSerialization: boolean;
  numPartitions: number | null;
  timestamp: number;
  partitionSetName: string | null;
  partitionSet: {
    __typename: 'PartitionSet';
    id: string;
    name: string;
    mode: string;
    pipelineName: string;
    repositoryOrigin: {
      __typename: 'RepositoryOrigin';
      id: string;
      repositoryName: string;
      repositoryLocationName: string;
    };
  } | null;
  assetSelection: Array<{__typename: 'AssetKey'; path: Array<string>}> | null;
  error: {
    __typename: 'PythonError';
    message: string;
    stack: Array<string>;
    errorChain: Array<{
      __typename: 'ErrorChainLink';
      isExplicitLink: boolean;
      error: {__typename: 'PythonError'; message: string; stack: Array<string>};
    }>;
  } | null;
};

export type PartitionSetForBackfillTableFragment = {
  __typename: 'PartitionSet';
  id: string;
  name: string;
  mode: string;
  pipelineName: string;
  repositoryOrigin: {
    __typename: 'RepositoryOrigin';
    id: string;
    repositoryName: string;
    repositoryLocationName: string;
  };
};
