defmodule Postgrex.PgOutput.Type do
  @moduledoc false

  # Generates Postgrex.Type info from pg types in types.exs
  # The mix task: postgrex.pg_output.types is used to generate it from
  # a types.json fetched from pg.

  @external_resource pg_types_path =
                       Application.compile_env(
                         :postgrex_pgoutput,
                         :pg_types_path,
                         Path.join(__DIR__, "types.exs")
                       )

  @json_lib Application.compile_env(:postgrex, :json_library, Jason)
  {types, _} = Code.eval_file(pg_types_path)

  pg_types = for type <- types, do: struct(Postgrex.TypeInfo, type)

  # ETS table name for runtime types
  @runtime_types_table :postgrex_pgoutput_runtime_types

  # Store compile time types for reference
  @compiled_time_types pg_types
  def compiled_time_types, do: @compiled_time_types

  @spec has_runtime_types?() :: boolean()
  @doc """
  Returns if any runtime types have been loaded
  """
  def has_runtime_types? do
    ets_table_exists?() and :ets.info(@runtime_types_table, :size) > 0
  end

  for type = %{oid: oid, type: type_name} <- pg_types do
    def type_info_compiled(unquote(type_name)), do: unquote(Macro.escape(type))
    def oid_to_info_compiled(unquote(oid)), do: unquote(Macro.escape(type))
  end

  # Fallback functions for compile-time lookups
  def type_info_compiled(_type_name), do: nil
  def oid_to_info_compiled(_oid), do: nil

  def type_info(type_name) do
    # First check compile-time types
    case type_info_compiled(type_name) do
      nil ->
        # Fall back to runtime types if the table exists
        if ets_table_exists?() do
          case :ets.lookup(@runtime_types_table, {:type, type_name}) do
            [{_, type_info}] -> type_info
            [] -> nil
          end
        else
          nil
        end

      type_info ->
        type_info
    end
  end

  def oid_to_info(oid) do
    # First check compile-time types
    case oid_to_info_compiled(oid) do
      nil ->
        # Fall back to runtime types if the table exists
        if ets_table_exists?() do
          case :ets.lookup(@runtime_types_table, {:oid, oid}) do
            [{_, type_info}] -> type_info
            [] -> nil
          end
        else
          nil
        end

      type_info ->
        type_info
    end
  end

  # Helper to safely check if the ETS table exists
  defp ets_table_exists? do
    :ets.whereis(@runtime_types_table) != :undefined
  end

  @doc """
  Returns all types, compiled and runtime loaded types
  """
  def all_types do
    compiled_time_types = @compiled_time_types

    # Get all runtime types if table exists
    runtime_types =
      if ets_table_exists?() do
        :ets.select(@runtime_types_table, [{{{:type, :_}, :"$1"}, [], [:"$1"]}])
      else
        []
      end

    # Return the combined list of unique types
    compiled_time_types ++ runtime_types
  end

  @doc """
  Loads PostgreSQL type information from a types file at runtime.
  """
  def load_runtime_types(types_file) do
    # Ensure the ETS table exists
    ensure_ets_table()

    with {:ok, bin} <- File.read(types_file),
         {types, _} <- Code.eval_string(bin) do
      # Ensure types are valid before processing
      case validate_types(types) do
        {:ok, valid_types} ->
          # Convert to TypeInfo structs
          type_infos = Enum.map(valid_types, &struct(Postgrex.TypeInfo, &1))

          # Store in ETS table
          for type_info = %{oid: oid, type: type_name} <- type_infos do
            :ets.insert(@runtime_types_table, {{:type, type_name}, type_info})
            :ets.insert(@runtime_types_table, {{:oid, oid}, type_info})
          end

          {:ok, length(type_infos)}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, reason} -> {:error, {:file_read_error, reason}}
      error -> {:error, {:unexpected_error, error}}
    end
  end

  # Ensure the ETS table exists
  defp ensure_ets_table do
    if :ets.whereis(@runtime_types_table) == :undefined do
      :ets.new(@runtime_types_table, [:named_table, :set, :public])
    end
  end

  # Validate the structure of loaded types
  defp validate_types(types) when is_list(types) do
    invalid_types =
      Enum.filter(types, fn type ->
        not (is_map(type) and Map.has_key?(type, :oid) and Map.has_key?(type, :type))
      end)

    if invalid_types == [] do
      {:ok, types}
    else
      {:error, {:invalid_types, invalid_types}}
    end
  end

  defp validate_types(types), do: {:error, {:invalid_format, types}}

  @doc """
  Clears the loaded runtime types from the ets table
  """
  def clear_runtime_types do
    if ets_table_exists?() do
      :ets.delete_all_objects(@runtime_types_table)
      :ok
    else
      {:ok, :no_runtime_types}
    end
  end

  @json_delim_pattern ~s(\",\")
  @delim_pattern ","
  def decode(nil, _), do: nil

  def decode(<<?{, bin::binary>>, %{
        send: "array_send",
        type: <<?_, type::binary>>,
        array_elem: array_elem
      }) do
    {pattern, unescape} = type_decode_opts(type)

    inner_type = oid_to_info(array_elem)

    decoded_array = decode_json_array(bin, pattern, unescape, [])

    decoded_array
    |> Enum.map(&decode(&1, inner_type))
    |> Enum.reverse()
  end

  for type <- ["varchar", "timestamp", "timestamptz", "uuid", "text"] do
    def decode(data, %{type: unquote(type)}) do
      data
    end
  end

  for type <- ["int2", "int4", "int8"] do
    def decode(data, %{type: unquote(type)}) do
      {int, _} = Integer.parse(data)
      int
    end
  end

  for type <- ["float4", "float8"] do
    def decode(data, %{type: unquote(type)}) do
      {float, _} = Float.parse(data)
      float
    end
  end

  for type <- ["json", "jsonb"] do
    def decode(data, %{type: unquote(type)}) do
      if json_lib = load_jsonlib() do
        json_lib.decode!(data)
      else
        raise "no `:json_library` configured"
      end
    end
  end

  def decode("t", %{type: "bool"}), do: true
  def decode("f", %{type: "bool"}), do: false
  def decode(date, %{type: "date"}), do: Date.from_iso8601!(date)
  def decode(time, %{type: "time"}), do: Time.from_iso8601!(time)

  def decode(value, type) do
    IO.warn(
      "no #{__MODULE__}.decode type implementation: #{inspect(type)} data: #{inspect(value)}"
    )

    value
  end

  def decode_json_array(pg_array, pattern, unescape, acc) do
    case :binary.match(pg_array, pattern) do
      :nomatch ->
        [pg_array |> String.trim_trailing("}") |> unescape.() | acc]

      {pos, _len} ->
        n = byte_size(pattern)
        {value, <<_p::binary-size(n), rest::binary>>} = :erlang.split_binary(pg_array, pos)

        decode_json_array(rest, pattern, unescape, [unescape.(value) | acc])
    end
  end

  defp type_decode_opts(type) when type in ~w(json jsonb),
    do: {@json_delim_pattern, &unescape_json/1}

  defp type_decode_opts(_type), do: {@delim_pattern, &Function.identity/1}

  defp unescape_json(json) do
    json
    |> String.trim(<<?">>)
    |> String.replace("\\", "")
  end

  defp load_jsonlib do
    Code.ensure_loaded?(@json_lib) and @json_lib
  end
end
