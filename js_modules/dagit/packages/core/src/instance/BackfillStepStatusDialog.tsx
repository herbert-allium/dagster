import {Button, DialogFooter, Dialog} from '@dagster-io/ui';
import * as React from 'react';

import {PartitionPerOpStatus} from '../partitions/PartitionStepStatus';
import {usePartitionStepQuery} from '../partitions/usePartitionStepQuery';
import {DagsterTag} from '../runs/RunTag';
import {RunFilterToken} from '../runs/RunsFilterInput';
import {buildRepoAddress} from '../workspace/buildRepoAddress';
import {repoAddressToSelector} from '../workspace/repoAddressToSelector';
import {RepoAddress} from '../workspace/types';

import {
  BackfillTableFragment,
  PartitionSetForBackfillTableFragment,
} from './types/BackfillTable.types';

interface Props {
  backfill?: BackfillTableFragment;
  onClose: () => void;
}

export const BackfillStepStatusDialog = ({backfill, onClose}: Props) => {
  const content = () => {
    if (!backfill?.partitionSet || backfill.partitionNames === null) {
      return null;
    }

    const repoAddress = buildRepoAddress(
      backfill.partitionSet.repositoryOrigin.repositoryName,
      backfill.partitionSet.repositoryOrigin.repositoryLocationName,
    );

    return (
      <BackfillStepStatusDialogContent
        backfill={backfill}
        partitionSet={backfill.partitionSet}
        partitionNames={backfill.partitionNames}
        repoAddress={repoAddress}
        onClose={onClose}
      />
    );
  };

  return (
    <Dialog
      isOpen={!!backfill?.partitionSet}
      title={`Step status for backfill: ${backfill?.backfillId}`}
      onClose={onClose}
      style={{width: '80vw'}}
    >
      {content()}
      <DialogFooter topBorder>
        <Button onClick={onClose}>Done</Button>
      </DialogFooter>
    </Dialog>
  );
};

interface ContentProps {
  backfill: BackfillTableFragment;
  partitionSet: PartitionSetForBackfillTableFragment;
  partitionNames: string[];
  repoAddress: RepoAddress;
  onClose: () => void;
}

export const BackfillStepStatusDialogContent = ({
  backfill,
  partitionSet,
  partitionNames,
  repoAddress,
}: ContentProps) => {
  const [pageSize, setPageSize] = React.useState(60);
  const [offset, setOffset] = React.useState<number>(0);

  const runsFilter = React.useMemo(() => {
    const token: RunFilterToken = {token: 'tag', value: `dagster/backfill=${backfill.backfillId}`};
    return [token];
  }, [backfill.backfillId]);

  const partitions = usePartitionStepQuery({
    partitionSetName: partitionSet.name,
    partitionTagName: DagsterTag.Partition,
    partitionNames,
    pageSize,
    runsFilter,
    repositorySelector: repoAddressToSelector(repoAddress),
    jobName: partitionSet.pipelineName,
    offset,
    skipQuery: !backfill,
  });

  return (
    <PartitionPerOpStatus
      partitionNames={partitionNames}
      partitions={partitions}
      pipelineName={partitionSet?.pipelineName}
      repoAddress={repoAddress}
      setPageSize={setPageSize}
      offset={offset}
      setOffset={setOffset}
    />
  );
};
