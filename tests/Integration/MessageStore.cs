using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Grpc.Core;
using NUnit.Framework;
using Polly;

namespace Integration;

public class MessageStore
{
    private readonly object _locker = new();
    private readonly List<(string address, object? message, Exception? exception)> _store = new();
    private readonly List<Channel<(string addres,object? message, Exception? exception)>> _readers = new();

    public async Task WriteAsync(string address, object? message, Exception? exception = null)
    {
        await Task.Run(
            () =>
            {
                lock (_locker)
                {
                    _store.Add((address, message, exception));
                    foreach (Channel<(string addres, object? message, Exception? exception)> channel in _readers)
                    {
                        channel.Writer.TryWrite((address, message, exception));
                    }
                }
            });
    }


    public async IAsyncEnumerable<(string address, object? message, Exception? exception)> ReadMessages()
    {

        Channel<(string addres,object? message, Exception? exception)>? channel = null;
        try
        {
            lock (_locker)
            {
                channel = Channel.CreateUnbounded<(string addres,object? message, Exception? exception)>();
                _readers.Add(channel);
                List<(string address, object? message, Exception? exception)> existingMessages = new(_store);
                foreach ((string address, object? message, Exception? exception) in existingMessages)
                {
                    channel.Writer.TryWrite((address, message, exception));
                }
            }

            await foreach ((string addres, object? message, Exception? exception) in channel.Reader.ReadAllAsync())
            {
                yield return (addres, message, exception);
            }
        }
        finally
        {
            lock (_locker)
            {
                if (channel != null)
                {
                    _readers.Remove(channel);
                }
            }
        }
    }

    public async IAsyncEnumerable<(object? message, Exception? exception)> ReadMessages(string address)
    {
        await foreach (var (addr, message, exception) in ReadMessages())
        {
            if (addr == address)
            {
                yield return (message, exception);
            }
        }
    }



    public IClientStreamWriter<T> GetWriter<T>(string address)
    {
        return new DelegateStreamWriter<T>(
            async message => await WriteAsync(address, message, null),
            async () =>
            {
            });
    }

    public IAsyncStreamReader<T> GetReader<T>(string address)
    {
        return new StreamReader<T>(ReadMessages(address).GetAsyncEnumerator());
    }
    
    
    
    private class DelegateStreamWriter<T> : IClientStreamWriter<T>
    {
        private readonly Func<T, Task> _onWrite;
        private readonly Func<Task> _onComplete;
    
        public DelegateStreamWriter(Func<T,Task> onWrite, Func<Task> onComplete)
        {
            _onWrite = onWrite;
            _onComplete = onComplete;
        }


        public async Task WriteAsync(T message)
        {
            await _onWrite(message);
        }
    

        public WriteOptions? WriteOptions { get; set; }
        public async Task CompleteAsync()
        {
            await _onComplete();
        }
    }

    private class StreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly IAsyncEnumerator<(object? message, Exception? exception)> _enumerator;
        private readonly Func<T> _getCurrentFunc;

        public StreamReader(IAsyncEnumerator<(object?, Exception?)> enumerator)
        {
            _enumerator = enumerator;
            _getCurrentFunc = () => _enumerator.Current.exception == null
                ? (T)_enumerator.Current.message!
                : throw _enumerator.Current.exception;
        }
        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            return await _enumerator.MoveNextAsync();
        }

        public T Current => _getCurrentFunc();
    }
}
