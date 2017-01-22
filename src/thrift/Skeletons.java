package thrift;

import com.twitter.scrooge.ScroogeOption;
import com.twitter.finagle.SourcedException;
import com.twitter.scrooge.ThriftStruct;
import com.twitter.scrooge.ThriftStructCodec;
import com.twitter.scrooge.Utilities;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import org.apache.thrift.protocol.*;
import org.apache.thrift.TApplicationException;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ThriftClientRequest;
import java.util.Arrays;
import org.apache.thrift.TException;
import com.twitter.finagle.Service;
import com.twitter.finagle.stats.Counter;
import com.twitter.util.Function;
import com.twitter.util.Function2;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.finagle.tracing.NullTracer;
import com.twitter.finagle.tracing.Tracer;
import com.twitter.logging.Logger;


public class Skeletons {
  public interface Iface {
    
    public Boolean append(String key, String value);
    
    public String get(String key);
    
    public Boolean clear(String key);
    
    public Boolean finalize(String key);
  }

  public interface FutureIface {
    
    public Future<Boolean> append(String key, String value);
    
    public Future<String> get(String key);
    
    public Future<Boolean> clear(String key);
    
    public Future<Boolean> finalize(String key);
  }

  static class append_args implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("append_args");
    private static final TField KeyField = new TField("key", TType.STRING, (short) 1);
    final String key;
    private static final TField ValueField = new TField("value", TType.STRING, (short) 2);
    final String value;
  
    public static class Builder {
      private String _key = null;
      private Boolean _got_key = false;
  
      public Builder key(String value) {
        this._key = value;
        this._got_key = true;
        return this;
      }
  
      public Builder unsetKey() {
        this._key = null;
        this._got_key = false;
        return this;
      }
      private String _value = null;
      private Boolean _got_value = false;
  
      public Builder value(String value) {
        this._value = value;
        this._got_value = true;
        return this;
      }
  
      public Builder unsetValue() {
        this._value = null;
        this._got_value = false;
        return this;
      }
  
      public append_args build() {
        return new append_args(
          this._key,
          this._value    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      builder.key(this.key);
      builder.value(this.value);
      return builder;
    }
  
    public static ThriftStructCodec<append_args> CODEC = new ThriftStructCodec<append_args>() {
      public append_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        String key = null;
        String value = null;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 1: /* key */
                switch (_field.type) {
                  case TType.STRING:
                    String key_item;
                    key_item = _iprot.readString();
                    key = key_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.key(key);
                break;
              case 2: /* value */
                switch (_field.type) {
                  case TType.STRING:
                    String value_item;
                    value_item = _iprot.readString();
                    value = value_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.value(value);
                break;
              default:
                TProtocolUtil.skip(_iprot, _field.type);
            }
            _iprot.readFieldEnd();
          }
        }
        _iprot.readStructEnd();
        try {
          return builder.build();
        } catch (IllegalStateException stateEx) {
          throw new TProtocolException(stateEx.getMessage());
        }
      }
  
      public void encode(append_args struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static append_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(append_args struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public append_args(
      String key, 
      String value
    ) {
      this.key = key;
      this.value = value;
    }
  
  
    public String getKey() {
      return this.key;
    }
    public boolean isSetKey() {
      return this.key != null;
    }
    public String getValue() {
      return this.value;
    }
    public boolean isSetValue() {
      return this.value != null;
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
        _oprot.writeFieldBegin(KeyField);
        String key_item = key;
        _oprot.writeString(key_item);
        _oprot.writeFieldEnd();
        _oprot.writeFieldBegin(ValueField);
        String value_item = value;
        _oprot.writeString(value_item);
        _oprot.writeFieldEnd();
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof append_args)) return false;
      append_args that = (append_args) other;
      return
  this.key.equals(that.key) &&
  this.value.equals(that.value);
    }
  
    public String toString() {
      return "append_args(" + this.key + "," + this.value + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.key == null ? 0 : this.key.hashCode());
      hash = hash * (this.value == null ? 0 : this.value.hashCode());
      return hash;
    }
  }
  static class append_result implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("append_result");
    private static final TField SuccessField = new TField("success", TType.BOOL, (short) 0);
    final ScroogeOption<Boolean> success;
  
    public static class Builder {
      private boolean _success = false;
      private Boolean _got_success = false;
  
      public Builder success(boolean value) {
        this._success = value;
        this._got_success = true;
        return this;
      }
  
      public Builder unsetSuccess() {
        this._success = false;
        this._got_success = false;
        return this;
      }
  
      public append_result build() {
        return new append_result(
        ScroogeOption.make(this._got_success, this._success)    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      if (this.success.isDefined()) builder.success(this.success.get());
      return builder;
    }
  
    public static ThriftStructCodec<append_result> CODEC = new ThriftStructCodec<append_result>() {
      public append_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        boolean success = false;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 0: /* success */
                switch (_field.type) {
                  case TType.BOOL:
                    Boolean success_item;
                    success_item = _iprot.readBool();
                    success = success_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.success(success);
                break;
              default:
                TProtocolUtil.skip(_iprot, _field.type);
            }
            _iprot.readFieldEnd();
          }
        }
        _iprot.readStructEnd();
        try {
          return builder.build();
        } catch (IllegalStateException stateEx) {
          throw new TProtocolException(stateEx.getMessage());
        }
      }
  
      public void encode(append_result struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static append_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(append_result struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public append_result(
      ScroogeOption<Boolean> success
    ) {
      this.success = success;
    }
  
  
    public boolean getSuccess() {
      return this.success.get();
    }
    public boolean isSetSuccess() {
      return this.success.isDefined();
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
      if (success.isDefined()) {  _oprot.writeFieldBegin(SuccessField);
        Boolean success_item = success.get();
        _oprot.writeBool(success_item);
        _oprot.writeFieldEnd();
      }
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof append_result)) return false;
      append_result that = (append_result) other;
      return
        this.success.equals(that.success)
  ;
    }
  
    public String toString() {
      return "append_result(" + this.success + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.success.isDefined() ? 0 : new Boolean(this.success.get()).hashCode());
      return hash;
    }
  }
  static class get_args implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("get_args");
    private static final TField KeyField = new TField("key", TType.STRING, (short) 1);
    final String key;
  
    public static class Builder {
      private String _key = null;
      private Boolean _got_key = false;
  
      public Builder key(String value) {
        this._key = value;
        this._got_key = true;
        return this;
      }
  
      public Builder unsetKey() {
        this._key = null;
        this._got_key = false;
        return this;
      }
  
      public get_args build() {
        return new get_args(
          this._key    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      builder.key(this.key);
      return builder;
    }
  
    public static ThriftStructCodec<get_args> CODEC = new ThriftStructCodec<get_args>() {
      public get_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        String key = null;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 1: /* key */
                switch (_field.type) {
                  case TType.STRING:
                    String key_item;
                    key_item = _iprot.readString();
                    key = key_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.key(key);
                break;
              default:
                TProtocolUtil.skip(_iprot, _field.type);
            }
            _iprot.readFieldEnd();
          }
        }
        _iprot.readStructEnd();
        try {
          return builder.build();
        } catch (IllegalStateException stateEx) {
          throw new TProtocolException(stateEx.getMessage());
        }
      }
  
      public void encode(get_args struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static get_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(get_args struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public get_args(
      String key
    ) {
      this.key = key;
    }
  
  
    public String getKey() {
      return this.key;
    }
    public boolean isSetKey() {
      return this.key != null;
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
        _oprot.writeFieldBegin(KeyField);
        String key_item = key;
        _oprot.writeString(key_item);
        _oprot.writeFieldEnd();
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof get_args)) return false;
      get_args that = (get_args) other;
      return
  this.key.equals(that.key);
    }
  
    public String toString() {
      return "get_args(" + this.key + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.key == null ? 0 : this.key.hashCode());
      return hash;
    }
  }
  static class get_result implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("get_result");
    private static final TField SuccessField = new TField("success", TType.STRING, (short) 0);
    final ScroogeOption<String> success;
  
    public static class Builder {
      private String _success = null;
      private Boolean _got_success = false;
  
      public Builder success(String value) {
        this._success = value;
        this._got_success = true;
        return this;
      }
  
      public Builder unsetSuccess() {
        this._success = null;
        this._got_success = false;
        return this;
      }
  
      public get_result build() {
        return new get_result(
        ScroogeOption.make(this._got_success, this._success)    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      if (this.success.isDefined()) builder.success(this.success.get());
      return builder;
    }
  
    public static ThriftStructCodec<get_result> CODEC = new ThriftStructCodec<get_result>() {
      public get_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        String success = null;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 0: /* success */
                switch (_field.type) {
                  case TType.STRING:
                    String success_item;
                    success_item = _iprot.readString();
                    success = success_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.success(success);
                break;
              default:
                TProtocolUtil.skip(_iprot, _field.type);
            }
            _iprot.readFieldEnd();
          }
        }
        _iprot.readStructEnd();
        try {
          return builder.build();
        } catch (IllegalStateException stateEx) {
          throw new TProtocolException(stateEx.getMessage());
        }
      }
  
      public void encode(get_result struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static get_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(get_result struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public get_result(
      ScroogeOption<String> success
    ) {
      this.success = success;
    }
  
  
    public String getSuccess() {
      return this.success.get();
    }
    public boolean isSetSuccess() {
      return this.success.isDefined();
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
      if (success.isDefined()) {  _oprot.writeFieldBegin(SuccessField);
        String success_item = success.get();
        _oprot.writeString(success_item);
        _oprot.writeFieldEnd();
      }
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof get_result)) return false;
      get_result that = (get_result) other;
      return
  this.success.equals(that.success);
    }
  
    public String toString() {
      return "get_result(" + this.success + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.success.isDefined() ? 0 : this.success.get().hashCode());
      return hash;
    }
  }
  static class clear_args implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("clear_args");
    private static final TField KeyField = new TField("key", TType.STRING, (short) 1);
    final String key;
  
    public static class Builder {
      private String _key = null;
      private Boolean _got_key = false;
  
      public Builder key(String value) {
        this._key = value;
        this._got_key = true;
        return this;
      }
  
      public Builder unsetKey() {
        this._key = null;
        this._got_key = false;
        return this;
      }
  
      public clear_args build() {
        return new clear_args(
          this._key    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      builder.key(this.key);
      return builder;
    }
  
    public static ThriftStructCodec<clear_args> CODEC = new ThriftStructCodec<clear_args>() {
      public clear_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        String key = null;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 1: /* key */
                switch (_field.type) {
                  case TType.STRING:
                    String key_item;
                    key_item = _iprot.readString();
                    key = key_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.key(key);
                break;
              default:
                TProtocolUtil.skip(_iprot, _field.type);
            }
            _iprot.readFieldEnd();
          }
        }
        _iprot.readStructEnd();
        try {
          return builder.build();
        } catch (IllegalStateException stateEx) {
          throw new TProtocolException(stateEx.getMessage());
        }
      }
  
      public void encode(clear_args struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static clear_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(clear_args struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public clear_args(
      String key
    ) {
      this.key = key;
    }
  
  
    public String getKey() {
      return this.key;
    }
    public boolean isSetKey() {
      return this.key != null;
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
        _oprot.writeFieldBegin(KeyField);
        String key_item = key;
        _oprot.writeString(key_item);
        _oprot.writeFieldEnd();
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof clear_args)) return false;
      clear_args that = (clear_args) other;
      return
  this.key.equals(that.key);
    }
  
    public String toString() {
      return "clear_args(" + this.key + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.key == null ? 0 : this.key.hashCode());
      return hash;
    }
  }
  static class clear_result implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("clear_result");
    private static final TField SuccessField = new TField("success", TType.BOOL, (short) 0);
    final ScroogeOption<Boolean> success;
  
    public static class Builder {
      private boolean _success = false;
      private Boolean _got_success = false;
  
      public Builder success(boolean value) {
        this._success = value;
        this._got_success = true;
        return this;
      }
  
      public Builder unsetSuccess() {
        this._success = false;
        this._got_success = false;
        return this;
      }
  
      public clear_result build() {
        return new clear_result(
        ScroogeOption.make(this._got_success, this._success)    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      if (this.success.isDefined()) builder.success(this.success.get());
      return builder;
    }
  
    public static ThriftStructCodec<clear_result> CODEC = new ThriftStructCodec<clear_result>() {
      public clear_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        boolean success = false;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 0: /* success */
                switch (_field.type) {
                  case TType.BOOL:
                    Boolean success_item;
                    success_item = _iprot.readBool();
                    success = success_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.success(success);
                break;
              default:
                TProtocolUtil.skip(_iprot, _field.type);
            }
            _iprot.readFieldEnd();
          }
        }
        _iprot.readStructEnd();
        try {
          return builder.build();
        } catch (IllegalStateException stateEx) {
          throw new TProtocolException(stateEx.getMessage());
        }
      }
  
      public void encode(clear_result struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static clear_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(clear_result struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public clear_result(
      ScroogeOption<Boolean> success
    ) {
      this.success = success;
    }
  
  
    public boolean getSuccess() {
      return this.success.get();
    }
    public boolean isSetSuccess() {
      return this.success.isDefined();
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
      if (success.isDefined()) {  _oprot.writeFieldBegin(SuccessField);
        Boolean success_item = success.get();
        _oprot.writeBool(success_item);
        _oprot.writeFieldEnd();
      }
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof clear_result)) return false;
      clear_result that = (clear_result) other;
      return
        this.success.equals(that.success)
  ;
    }
  
    public String toString() {
      return "clear_result(" + this.success + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.success.isDefined() ? 0 : new Boolean(this.success.get()).hashCode());
      return hash;
    }
  }
  static class finalize_args implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("finalize_args");
    private static final TField KeyField = new TField("key", TType.STRING, (short) 1);
    final String key;
  
    public static class Builder {
      private String _key = null;
      private Boolean _got_key = false;
  
      public Builder key(String value) {
        this._key = value;
        this._got_key = true;
        return this;
      }
  
      public Builder unsetKey() {
        this._key = null;
        this._got_key = false;
        return this;
      }
  
      public finalize_args build() {
        return new finalize_args(
          this._key    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      builder.key(this.key);
      return builder;
    }
  
    public static ThriftStructCodec<finalize_args> CODEC = new ThriftStructCodec<finalize_args>() {
      public finalize_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        String key = null;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 1: /* key */
                switch (_field.type) {
                  case TType.STRING:
                    String key_item;
                    key_item = _iprot.readString();
                    key = key_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.key(key);
                break;
              default:
                TProtocolUtil.skip(_iprot, _field.type);
            }
            _iprot.readFieldEnd();
          }
        }
        _iprot.readStructEnd();
        try {
          return builder.build();
        } catch (IllegalStateException stateEx) {
          throw new TProtocolException(stateEx.getMessage());
        }
      }
  
      public void encode(finalize_args struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static finalize_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(finalize_args struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public finalize_args(
      String key
    ) {
      this.key = key;
    }
  
  
    public String getKey() {
      return this.key;
    }
    public boolean isSetKey() {
      return this.key != null;
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
        _oprot.writeFieldBegin(KeyField);
        String key_item = key;
        _oprot.writeString(key_item);
        _oprot.writeFieldEnd();
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof finalize_args)) return false;
      finalize_args that = (finalize_args) other;
      return
  this.key.equals(that.key);
    }
  
    public String toString() {
      return "finalize_args(" + this.key + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.key == null ? 0 : this.key.hashCode());
      return hash;
    }
  }
  static class finalize_result implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("finalize_result");
    private static final TField SuccessField = new TField("success", TType.BOOL, (short) 0);
    final ScroogeOption<Boolean> success;
  
    public static class Builder {
      private boolean _success = false;
      private Boolean _got_success = false;
  
      public Builder success(boolean value) {
        this._success = value;
        this._got_success = true;
        return this;
      }
  
      public Builder unsetSuccess() {
        this._success = false;
        this._got_success = false;
        return this;
      }
  
      public finalize_result build() {
        return new finalize_result(
        ScroogeOption.make(this._got_success, this._success)    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      if (this.success.isDefined()) builder.success(this.success.get());
      return builder;
    }
  
    public static ThriftStructCodec<finalize_result> CODEC = new ThriftStructCodec<finalize_result>() {
      public finalize_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        boolean success = false;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 0: /* success */
                switch (_field.type) {
                  case TType.BOOL:
                    Boolean success_item;
                    success_item = _iprot.readBool();
                    success = success_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.success(success);
                break;
              default:
                TProtocolUtil.skip(_iprot, _field.type);
            }
            _iprot.readFieldEnd();
          }
        }
        _iprot.readStructEnd();
        try {
          return builder.build();
        } catch (IllegalStateException stateEx) {
          throw new TProtocolException(stateEx.getMessage());
        }
      }
  
      public void encode(finalize_result struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static finalize_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(finalize_result struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public finalize_result(
      ScroogeOption<Boolean> success
    ) {
      this.success = success;
    }
  
  
    public boolean getSuccess() {
      return this.success.get();
    }
    public boolean isSetSuccess() {
      return this.success.isDefined();
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
      if (success.isDefined()) {  _oprot.writeFieldBegin(SuccessField);
        Boolean success_item = success.get();
        _oprot.writeBool(success_item);
        _oprot.writeFieldEnd();
      }
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof finalize_result)) return false;
      finalize_result that = (finalize_result) other;
      return
        this.success.equals(that.success)
  ;
    }
  
    public String toString() {
      return "finalize_result(" + this.success + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.success.isDefined() ? 0 : new Boolean(this.success.get()).hashCode());
      return hash;
    }
  }
  
  public static class FinagledClient implements FutureIface {
    private com.twitter.finagle.Service<ThriftClientRequest, byte[]> service;
    private String serviceName;
    private TProtocolFactory protocolFactory /* new TBinaryProtocol.Factory */;
    private StatsReceiver scopedStats;
  
    public FinagledClient(
      com.twitter.finagle.Service<ThriftClientRequest, byte[]> service,
      TProtocolFactory protocolFactory /* new TBinaryProtocol.Factory */,
      String serviceName,
      StatsReceiver stats
    ) {
      this.service = service;
      this.serviceName = serviceName;
      this.protocolFactory = protocolFactory;
      if (serviceName != "") {
        this.scopedStats = stats.scope(serviceName);
      } else {
        this.scopedStats = stats;
      }
    }
  
    // ----- boilerplate that should eventually be moved into finagle:
  
    protected ThriftClientRequest encodeRequest(String name, ThriftStruct args) {
      TMemoryBuffer buf = new TMemoryBuffer(512);
      TProtocol oprot = protocolFactory.getProtocol(buf);
  
      try {
        oprot.writeMessageBegin(new TMessage(name, TMessageType.CALL, 0));
        args.write(oprot);
        oprot.writeMessageEnd();
      } catch (TException e) {
        // not real.
      }
  
      byte[] bytes = Arrays.copyOfRange(buf.getArray(), 0, buf.length());
      return new ThriftClientRequest(bytes, false);
    }
  
    protected <T extends ThriftStruct> T decodeResponse(byte[] resBytes, ThriftStructCodec<T> codec) throws TException {
      TProtocol iprot = protocolFactory.getProtocol(new TMemoryInputTransport(resBytes));
      TMessage msg = iprot.readMessageBegin();
      try {
        if (msg.type == TMessageType.EXCEPTION) {
          TException exception = TApplicationException.read(iprot);
          if (exception instanceof SourcedException) {
            if (this.serviceName != "") ((SourcedException) exception).serviceName_$eq(this.serviceName);
          }
          throw exception;
        } else {
          return codec.decode(iprot);
        }
      } finally {
        iprot.readMessageEnd();
      }
    }
  
    protected Exception missingResult(String name) {
      return new TApplicationException(
        TApplicationException.MISSING_RESULT,
        "`" + name + "` failed: unknown result"
      );
    }

  
    public Future<Boolean> append(String key, String value) {  
      Future<Boolean> rv = this.service.apply(encodeRequest("append", new append_args(key, value))).flatMap(new Function<byte[], Future<Boolean>>() {
        public Future<Boolean> apply(byte[] in) {
          try {
            append_result result = decodeResponse(in, append_result.CODEC);
  
  
            if (result.success.isDefined()) return Future.value(result.success.get());
            return Future.exception(missingResult("append"));
          } catch (TException e) {
            return Future.exception(e);
          }
        }
      }).rescue(new Function<Throwable, Future<Boolean>>() {
        public Future<Boolean> apply(Throwable t) {
          if (t instanceof SourcedException) {
            ((SourcedException) t).serviceName_$eq(FinagledClient.this.serviceName);
          }
          return Future.exception(t);
        }
      });
  
      rv.addEventListener(new FutureEventListener<Boolean>() {
        public void onSuccess(Boolean result) {
        }
  
        public void onFailure(Throwable t) {

        }
      });
  
      return rv;
    }
  
  
    public Future<String> get(String key) {  
      Future<String> rv = this.service.apply(encodeRequest("get", new get_args(key))).flatMap(new Function<byte[], Future<String>>() {
        public Future<String> apply(byte[] in) {
          try {
            get_result result = decodeResponse(in, get_result.CODEC);
  
  
            if (result.success.isDefined()) return Future.value(result.success.get());
            return Future.exception(missingResult("get"));
          } catch (TException e) {
            return Future.exception(e);
          }
        }
      }).rescue(new Function<Throwable, Future<String>>() {
        public Future<String> apply(Throwable t) {
          if (t instanceof SourcedException) {
            ((SourcedException) t).serviceName_$eq(FinagledClient.this.serviceName);
          }
          return Future.exception(t);
        }
      });
  
      rv.addEventListener(new FutureEventListener<String>() {
        public void onSuccess(String result) {
        }
  
        public void onFailure(Throwable t) {

        }
      });
  
      return rv;
    }
   
    public Future<Boolean> clear(String key) {  
      Future<Boolean> rv = this.service.apply(encodeRequest("clear", new clear_args(key))).flatMap(new Function<byte[], Future<Boolean>>() {
        public Future<Boolean> apply(byte[] in) {
          try {
            clear_result result = decodeResponse(in, clear_result.CODEC);
  
  
            if (result.success.isDefined()) return Future.value(result.success.get());
            return Future.exception(missingResult("clear"));
          } catch (TException e) {
            return Future.exception(e);
          }
        }
      }).rescue(new Function<Throwable, Future<Boolean>>() {
        public Future<Boolean> apply(Throwable t) {
          if (t instanceof SourcedException) {
            ((SourcedException) t).serviceName_$eq(FinagledClient.this.serviceName);
          }
          return Future.exception(t);
        }
      });
  
      rv.addEventListener(new FutureEventListener<Boolean>() {
        public void onSuccess(Boolean result) {
        }
  
        public void onFailure(Throwable t) {

        }
      });
  
      return rv;
    }  
  
    public Future<Boolean> finalize(String key) {  
      Future<Boolean> rv = this.service.apply(encodeRequest("finalize", new finalize_args(key))).flatMap(new Function<byte[], Future<Boolean>>() {
        public Future<Boolean> apply(byte[] in) {
          try {
            finalize_result result = decodeResponse(in, finalize_result.CODEC);
  
  
            if (result.success.isDefined()) return Future.value(result.success.get());
            return Future.exception(missingResult("finalize"));
          } catch (TException e) {
            return Future.exception(e);
          }
        }
      }).rescue(new Function<Throwable, Future<Boolean>>() {
        public Future<Boolean> apply(Throwable t) {
          if (t instanceof SourcedException) {
            ((SourcedException) t).serviceName_$eq(FinagledClient.this.serviceName);
          }
          return Future.exception(t);
        }
      });
  
      rv.addEventListener(new FutureEventListener<Boolean>() {
        public void onSuccess(Boolean result) {
        }
  
        public void onFailure(Throwable t) {
        }
      });
  
      return rv;
    }
  }
  
  public static class FinagledService extends Service<byte[], byte[]> {
    final private FutureIface iface;
    final private TProtocolFactory protocolFactory;
  
    public FinagledService(final FutureIface iface, final TProtocolFactory protocolFactory) {
      this.iface = iface;
      this.protocolFactory = protocolFactory;
  
      addFunction("append", new Function2<TProtocol, Integer, Future<byte[]>>() {
        public Future<byte[]> apply(TProtocol iprot, final Integer seqid) {
          try {
            append_args args = append_args.decode(iprot);
            iprot.readMessageEnd();
            Future<Boolean> result;
            try {
              result = iface.append(args.key, args.value);
            } catch (Throwable t) {
              result = Future.exception(t);
            }
            return result.flatMap(new Function<Boolean, Future<byte[]>>() {
              public Future<byte[]> apply(Boolean value){
                return reply("append", seqid, new append_result.Builder().success(value).build());
              }
            }).rescue(new Function<Throwable, Future<byte[]>>() {
              public Future<byte[]> apply(Throwable t) {
                return Future.exception(t);
              }
            });
          } catch (TProtocolException e) {
            try {
              iprot.readMessageEnd();
              return exception("append", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage());
            } catch (Exception unrecoverable) {
              return Future.exception(unrecoverable);
            }
          } catch (Throwable t) {
            return Future.exception(t);
          }
        }
      });
      addFunction("get", new Function2<TProtocol, Integer, Future<byte[]>>() {
        public Future<byte[]> apply(TProtocol iprot, final Integer seqid) {
          try {
            get_args args = get_args.decode(iprot);
            iprot.readMessageEnd();
            Future<String> result;
            try {
              result = iface.get(args.key);
            } catch (Throwable t) {
              result = Future.exception(t);
            }
            return result.flatMap(new Function<String, Future<byte[]>>() {
              public Future<byte[]> apply(String value){
                return reply("get", seqid, new get_result.Builder().success(value).build());
              }
            }).rescue(new Function<Throwable, Future<byte[]>>() {
              public Future<byte[]> apply(Throwable t) {
                return Future.exception(t);
              }
            });
          } catch (TProtocolException e) {
            try {
              iprot.readMessageEnd();
              return exception("get", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage());
            } catch (Exception unrecoverable) {
              return Future.exception(unrecoverable);
            }
          } catch (Throwable t) {
            return Future.exception(t);
          }
        }
      });
      addFunction("clear", new Function2<TProtocol, Integer, Future<byte[]>>() {
        public Future<byte[]> apply(TProtocol iprot, final Integer seqid) {
          try {
            clear_args args = clear_args.decode(iprot);
            iprot.readMessageEnd();
            Future<Boolean> result;
            try {
              result = iface.clear(args.key);
            } catch (Throwable t) {
              result = Future.exception(t);
            }
            return result.flatMap(new Function<Boolean, Future<byte[]>>() {
              public Future<byte[]> apply(Boolean value){
                return reply("clear", seqid, new clear_result.Builder().success(value).build());
              }
            }).rescue(new Function<Throwable, Future<byte[]>>() {
              public Future<byte[]> apply(Throwable t) {
                return Future.exception(t);
              }
            });
          } catch (TProtocolException e) {
            try {
              iprot.readMessageEnd();
              return exception("clear", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage());
            } catch (Exception unrecoverable) {
              return Future.exception(unrecoverable);
            }
          } catch (Throwable t) {
            return Future.exception(t);
          }
        }
      });
      addFunction("finalize", new Function2<TProtocol, Integer, Future<byte[]>>() {
        public Future<byte[]> apply(TProtocol iprot, final Integer seqid) {
          try {
            finalize_args args = finalize_args.decode(iprot);
            iprot.readMessageEnd();
            Future<Boolean> result;
            try {
              result = iface.finalize(args.key);
            } catch (Throwable t) {
              result = Future.exception(t);
            }
            return result.flatMap(new Function<Boolean, Future<byte[]>>() {
              public Future<byte[]> apply(Boolean value){
                return reply("finalize", seqid, new finalize_result.Builder().success(value).build());
              }
            }).rescue(new Function<Throwable, Future<byte[]>>() {
              public Future<byte[]> apply(Throwable t) {
                return Future.exception(t);
              }
            });
          } catch (TProtocolException e) {
            try {
              iprot.readMessageEnd();
              return exception("finalize", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage());
            } catch (Exception unrecoverable) {
              return Future.exception(unrecoverable);
            }
          } catch (Throwable t) {
            return Future.exception(t);
          }
        }
      });
    }
  
    // ----- boilerplate that should eventually be moved into finagle:
  
    protected Map<String, Function2<TProtocol, Integer, Future<byte[]>>> functionMap =
      new HashMap<String, Function2<TProtocol, Integer, Future<byte[]>>>();
  
    protected void addFunction(String name, Function2<TProtocol, Integer, Future<byte[]>> fn) {
      functionMap.put(name, fn);
    }
  
    protected Function2<TProtocol, Integer, Future<byte[]>> getFunction(String name) {
      return functionMap.get(name);
    }
  
    protected Future<byte[]> exception(String name, int seqid, int code, String message) {
      try {
        TApplicationException x = new TApplicationException(code, message);
        TMemoryBuffer memoryBuffer = new TMemoryBuffer(512);
        TProtocol oprot = protocolFactory.getProtocol(memoryBuffer);
  
        oprot.writeMessageBegin(new TMessage(name, TMessageType.EXCEPTION, seqid));
        x.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
        return Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()));
      } catch (Exception e) {
        return Future.exception(e);
      }
    }
  
    protected Future<byte[]> reply(String name, int seqid, ThriftStruct result) {
      try {
        TMemoryBuffer memoryBuffer = new TMemoryBuffer(512);
        TProtocol oprot = protocolFactory.getProtocol(memoryBuffer);
  
        oprot.writeMessageBegin(new TMessage(name, TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
  
        return Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()));
      } catch (Exception e) {
        return Future.exception(e);
      }
    }
  
    public final Future<byte[]> apply(byte[] request) {
      TTransport inputTransport = new TMemoryInputTransport(request);
      TProtocol iprot = protocolFactory.getProtocol(inputTransport);
  
      try {
        TMessage msg = iprot.readMessageBegin();
        Function2<TProtocol, Integer, Future<byte[]>> f = functionMap.get(msg.name);
        if (f != null) {
          return f.apply(iprot, msg.seqid);
        } else {
          TProtocolUtil.skip(iprot, TType.STRUCT);
          return exception(msg.name, msg.seqid, TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
        }
      } catch (Exception e) {
        return Future.exception(e);
      }
    }
  
    // ---- end boilerplate.
  }
}