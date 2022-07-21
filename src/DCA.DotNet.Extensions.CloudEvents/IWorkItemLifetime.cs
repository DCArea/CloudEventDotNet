namespace DCA.DotNet.Extensions.CloudEvents;

internal interface IWorkItemLifetime<TWorkItem>
{
    ValueTask OnProcessed(TWorkItem workItem);
    ValueTask OnFinished(TWorkItem workItem);
}
