using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Grpc.Core;

namespace Integration;

public class MessageChannel<T> : IClientStreamWriter<T>, IAsyncStreamReader<T>
{
    private Channel<(object, Exception?)> _channel = Channel.CreateUnbounded<(object, Exception?)>();
    private (object message, Exception? exception) _current;
    private Func<T> GetCurrentFunc { get; set; }
    private Action<(object message, Exception? exception)> SetCurrentFunc { get; set; }
    public MessageChannel()
    {
        GetCurrentFunc = () =>
        {
            if (_current.exception != null) throw _current.exception;
            return (T)_current.message!;
        };
        SetCurrentFunc = current => _current = current;
    }
    
    public MessageChannel<TNew> CastMessageType<TNew>()
    {
        var ch = new MessageChannel<TNew>();
        ch._channel = _channel;
        ch.GetCurrentFunc = () =>
        {
            object current = GetCurrentFunc()!;
            return (TNew)current;
        };
        ch.SetCurrentFunc = this.SetCurrentFunc;
        return ch;
    }

    #region IClientStreamWriter

    public async Task WriteAsync(T message)
    {
        await _channel.Writer.WriteAsync((message, null)!);
    }

    public async Task WriteExceptionAsync(Exception exception)
    {
        await _channel.Writer.WriteAsync((null, exception)!);
    }

    public WriteOptions? WriteOptions { get; set; }

    public async Task CompleteAsync()
    {
        _channel.Writer.Complete();
    }

    #endregion

    #region IAsyncStreamReader

    public async Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        var haveNext = await _channel.Reader.WaitToReadAsync(cancellationToken);
        if (!haveNext) return false;
        var current = await _channel.Reader.ReadAsync(cancellationToken);
        SetCurrentFunc(current);
        return true;
    }
    public T Current => GetCurrentFunc();

    #endregion
}