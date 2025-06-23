defmodule Kino.DataTable do
  @moduledoc """
  A kino for interactively viewing tabular data.

  ## Examples

      data = [
        %{id: 1, name: "Elixir", website: "https://elixir-lang.org"},
        %{id: 2, name: "Erlang", website: "https://www.erlang.org"}
      ]

      Kino.DataTable.new(data)

  The tabular view allows you to quickly preview the data
  and analyze it thanks to sorting capabilities.

      data =
        for pid <- Process.list() do
          pid |> Process.info() |> Keyword.merge(registered_name: nil)
        end

      Kino.DataTable.new(
        data,
        keys: [:registered_name, :initial_call, :reductions, :stack_size]
      )
  """

  @behaviour Kino.Table

  @type t :: Kino.Table.t()

  @doc """
  Creates a new kino displaying given tabular data.

  ## Options

    * `:keys` - a list of keys to include in the table for each record.
      The order is reflected in the rendered table. Optional

    * `:name` - The displayed name of the table. Defaults to `"Data"`

    * `:sorting_enabled` - whether the table should support sorting the
      data. Sorting requires traversal of the whole enumerable, so it
      may not be desirable for large lazy enumerables. Defaults to `true`

   * `:formatter` - a 2-arity function that is used to format the data
     in the table. The first parameter passed is the `key` (column name) and
     the second is the value to be formatted. When formatting column headings
     the key is the special value `:__header__`. The formatter function must
     return either `{:ok, string}` or `:default`. When the return value is
     `:default` the default data table formatting is applied.

    * `:num_rows` - the number of rows to show in the table. Defaults to `10`.

  """
  @spec new(Table.Reader.t(), keyword()) :: t()
  def new(tabular, opts \\ []) do
    name = Keyword.get(opts, :name, "Data")
    sorting_enabled = Keyword.get(opts, :sorting_enabled, true)
    pagination_enabled = Keyword.get(opts, :pagination_enabled, true)
    edit_callback = Keyword.get(opts, :edit_callback, true)
    formatter = Keyword.get(opts, :formatter)
    num_rows = Keyword.get(opts, :num_rows)
    {data_rows, data_columns, count, inspected} = prepare_data(tabular, opts)

    Kino.Table.new(
      __MODULE__,
      {data_rows, data_columns, count, name, sorting_enabled, pagination_enabled, edit_callback,
       inspected, formatter, num_rows},
      export: fn state -> {"text", state.inspected} end
    )
  end

  @doc """
  Updates the table to display a new tabular data.

  ## Options

    * `:keys` - a list of keys to include in the table for each record.
      The order is reflected in the rendered table. Optional

  ## Examples

      data = [
        %{id: 1, name: "Elixir", website: "https://elixir-lang.org"},
        %{id: 2, name: "Erlang", website: "https://www.erlang.org"}
      ]

      kino = Kino.DataTable.new(data)

  Once created, you can update the table to display new data:

      new_data = [
        %{id: 1, name: "Elixir Lang", website: "https://elixir-lang.org"},
        %{id: 2, name: "Erlang Lang", website: "https://www.erlang.org"}
      ]

      Kino.DataTable.update(kino, new_data)
  """
  def update(kino, tabular, opts \\ []) do
    {data_rows, data_columns, count, inspected} = prepare_data(tabular, opts)
    Kino.Table.update(kino, {data_rows, data_columns, count, inspected})
  end

  defp prepare_data(tabular, opts) do
    tabular = normalize_tabular(tabular)
    keys = opts[:keys]

    # IO.inspect(keys, label: "Test")

    {_, meta, _} = reader = init_reader!(tabular)

    count = meta[:count] || infer_count(reader, tabular)

    {data_rows, data_columns} =
      if keys do
        data = Table.to_rows(reader, only: keys)
        nonexistent = keys -- meta.columns
        {data, keys -- nonexistent}
      else
        data = Table.to_rows(reader)
        {data, meta.columns}
      end

    inspected = inspect(tabular)

    {data_rows, data_columns, count, inspected}
  end

  defp normalize_tabular([%struct{} | _] = tabular) do
    Enum.map(tabular, fn
      %^struct{} = item ->
        Map.reject(item, fn {key, _val} ->
          key |> Atom.to_string() |> String.starts_with?("_")
        end)

      other ->
        raise ArgumentError,
              "expected a list of %#{inspect(struct)}{} structs, but got: #{inspect(other)}"
    end)
  end

  defp normalize_tabular(tabular), do: tabular

  defp init_reader!(tabular) do
    with :none <- Table.Reader.init(tabular) do
      raise ArgumentError, "expected valid tabular data, but got: #{inspect(tabular)}"
    end
  end

  defp infer_count({_, %{count: count}, _}, _), do: count

  # Handle lists as common cases for rows
  defp infer_count({:rows, _, _}, tabular) when is_list(tabular), do: length(tabular)
  defp infer_count({:rows, _, enum}, _) when is_list(enum), do: length(enum)

  # Handle kw/maps as common cases for columns
  defp infer_count({:columns, _, _}, [{_, series} | _]) when is_list(series), do: length(series)

  defp infer_count({:columns, _, _}, %{} = tabular) when not is_map_key(tabular, :__struct__) do
    case Enum.at(tabular, 0) do
      {_, series} when is_list(series) -> length(series)
      _ -> nil
    end
  end

  # Otherwise fallback to enumerable operations
  defp infer_count({:rows, _, enum}, _) do
    case Enumerable.count(enum) do
      {:ok, count} -> count
      _ -> nil
    end
  end

  defp infer_count({:columns, _, enum}, _) do
    with {:ok, series} <- Enum.fetch(enum, 0),
         {:ok, count} <- Enumerable.count(series),
         do: count,
         else: (_ -> nil)
  end

  # %{"cell" => [1, 0], "newValue" => %{"allowOverlay" => true, "allowWrapping" => false, "data" => "DIS A3937test", "displayData" => "DIS A3937", "kind" => "text", "readonly" => false}}, %Kino.JS.Live.Context{assigns: %{info: %{name: "Articles", features: [:sorting], num_rows: 300}, module: Kino.DataTable, state: %{formatter: nil, columns: [%{label: "id", key: "id"}, %{label: "deliverer_code", key: "deliverer_code"}, %{label: "deliverer_name", key: "deliverer_name"}, %{label: "deliverer_description", key: "deliverer_description"}, %{label: "intern_code", key: "intern_code"}, %{label: "intern_name", key: "intern_name"}, %{label: "intern_description", key: "intern_description"}, %{label: "purchase_price", key: "purchase_price"}, %{label: "sale_price", key: "sale_price"}, %{label: "purchase_total", key: "purchase_total"}, %{label: "sale_total", key: "sale_total"}, %{label: "is_stock", key: "is_stock"}, %{label: "stock_id", key: "stock_id"}, %{label: "stock_place_id", key: "stock_place_id"}, %{label: "stock_nr", key: "stock_nr"}, %{label: "stock_min_nr", key: "stock_min_nr"}, %{label: "stock_reserved_nr", key: "stock_reserved_nr"}, %{label: "stock_available_nr", key: "stock_available_nr"}, %{label: "stock_picked_nr", key: "stock_picked_nr"}, %{label: "stock_disposed_nr", key: "stock_disposed_nr"}, %{label: "stock_ordered_nr", key: "stock_ordered_nr"}, %{label: "html_text", key: "html_text"}, %{label: "remarks", key: "remarks"}, %{label: "delivery_price", key: "delivery_price"}], total_rows: 238, edit_callback: &IO.inspect/1, data_rows: %Table.Mapper{enumerable: [%{"deliverer_code" => "DIS A3937", "currency_id" => 1, "article_group_id" => nil, "html_text" => nil, "stock_available_nr" => 47, "stock_place_id" => nil, "stock_id" => 1, "intern_name" => "African Olive Seidenfoulard, 68x68", "tax_expense_id" => 22, "user_id" => 1, "stock_min_nr" => 5, "stock_disposed_nr" => 0, "stock_reserved_nr" => 0, "sale_total" => nil, "expense_account_id" => 174, "width" => 68, "stock_nr" => 47, "is_stock" => true, "tax_id" => 43, "stock_picked_nr" => 0, "deliverer_description" => "Druck auf nachhaltige Seide Twill 12MM mit Zertifikat.\r\nRollierte Kanten.\r\nEndformat: 68x68 cm\r\n\r\n68x68 cm fertig konfektioniert:\r\n60 St - € 23,90\r\n100 St - € 18,90\r\n\r\n90x90 cm fertig konfektioniert:\r\n60 St - € 45,00\r\n100 St - € 40,00", "article_type_id" => 1, "tax_income_id" => 43, "stock_ordered_nr" => 0, "account_id" => nil, "id" => 222, "purchase_price" => "17.000000", "deliverer_name" => "Foulard African Olive , ART B12148 GOTS SETA TWILL 12MM DIS A3937, 70x70", "contact_id" => 4, "purchase_total" => nil, "intern_description" => "<strong>Farbe</strong>: Gr&uuml;n/schwarz&nbsp;<strong>Gr&ouml;sse</strong>: 68 x 68 cm <strong>Verarbeitung</strong>: GOTS-Zert. Seide, rollierter Saum <strong>Herkunft</strong>: Designt in der Schweiz, hergestellt in Italien", "remarks" => "- Foulard Produktionskosten - Druck\r\n- Verpackung mit Seidepapier: 0.1 CHF\r\n- Verpackung mit Schachtel: 4.00 CHF\r\n\r\n- Foulard Grösse: 68x68 cm\r\n- Produktgrösse (verpackt): ?", "weight" => 25, "intern_code" => "DIS A3937", "unit_id" => 1, "sale_price" => "145.000000", "height" => 68, "volume" => nil, "delivery_price" => nil}, %{"deliverer_code" => "DIS A3937 (B-Quality)", "currency_id" => 1, "article_group_id" => nil, "html_text" => nil, "stock_available_nr" => 13, "stock_place_id" => nil, "stock_id" => 1, "intern_name" => "African Olive Seidenfoulard, 68x68 (B Quality)", "tax_expense_id" => 22, "user_id" => 1, "stock_min_nr" => 5, "stock_disposed_nr" => 0, "stock_reserved_nr" => 0, "sale_total" => nil, "expense_account_id" => 174, "width" => 68, "stock_nr" => 13, "is_stock" => true, "tax_id" => 43, "stock_picked_nr" => 0, "deliverer_description" => "Druck auf nachhaltige Seide Twill 12MM mit Zertifikat.\r\nRollierte Kanten.\r\nEndformat: 68x68 cm\r\n\r\n68x68 cm fertig konfektioniert:\r\n60 St - € 23,90 / B-Quality 16.73 EUR\r\n100 St - € 18,90\r\n\r\n90x90 cm fertig konfektioniert:\r\n60 St - € 45,00\r\n100 St - € 40,00", "article_type_id" => 1, "tax_income_id" => 43, "stock_ordered_nr" => 0, "account_id" => nil, "id" => 257, "purchase_price" => "17.000000", "deliverer_name" => "Foulard African Olive , ART B12148 GOTS SETA TWILL 12MM DIS A3937, 70x70", "contact_id" => 4, "purchase_total" => nil, "intern_description" => "Das Produkt enthaltet Druckfehler und/oder &auml;hnliches<strong> Farbe</strong>: Gr&uuml;n/schwarz&nbsp;<strong>Gr&ouml;sse</strong>: 68 x 68 cm <strong>Verarbeitung</strong>: GOTS-Zert. Seide, rollierter Saum <strong>Herkunft</strong>: Designt in der Schweiz, hergestellt in Italien", "remarks" => "- Foulard Produktionskosten - Druck\r\n- Verpackung mit Seidepapier: 0.1 CHF\r\n- Verpackung mit Schachtel: 4.00 CHF\r\n\r\n- Foulard Grösse: 68x68 cm\r\n- Produktgrösse (verpackt): ?", "weight" => 25, "intern_code" => "DIS A3937 B*", "unit_id" => 1, "sale_price" => "115.000000", "height" => 68, "volume" => nil, ...}, %{"deliverer_code" => "DIS A4203", "currency_id" => 1, "article_group_id" => nil, "html_text" => nil, "stock_available_nr" => 8, "stock_place_id" => nil, "stock_id" => 1, "intern_name" => "Amafu Seidenfoulard, 68x68", "tax_expense_id" => 22, "user_id" => 1, "stock_min_nr" => 5, "stock_disposed_nr" => 0, "stock_reserved_nr" => 0, "sale_total" => nil, "expense_account_id" => 174, "width" => 68, "stock_nr" => 12, "is_stock" => true, "tax_id" => 43, "stock_picked_nr" => 4, "deliverer_description" => "Druck auf nachhaltige Seide Twill 12MM mit Zertifikat.\r\nRollierte Kanten.\r\nEndformat: 68x68 cm\r\n\r\n68x68 cm fertig konfektioniert:\r\n60 St - € 23,90\r\n100 St - € 18,90\r\n\r\n90x90 cm fertig konfektioniert:\r\n60 St - € 45,00\r\n100 St - € 40,00", "article_type_id" => 1, "tax_income_id" => 43, "stock_ordered_nr" => 4, "account_id" => nil, "id" => 247, "purchase_price" => "24.000000", "deliverer_name" => "Foulard Amafu , ART B12148 GOTS SETA TWILL 12MM, DIS A4203, 70x70", "contact_id" => 4, "purchase_total" => nil, "intern_description" => "<strong>Farbe</strong>: hellblau&nbsp;<strong>Gr&ouml;sse</strong>: 68 x 68 cm <strong>Verarbeitung</strong>: GOTS-Zert. Seide, rollierter Saum <strong>Herkunft</strong>: Designt in der Schweiz, hergestellt in Italien", "remarks" => "- Foulard Produktionskosten - Druck\r\n- Verpackung mit Seidepapier: 0.1 CHF\r\n- Verpackung mit Schachtel: 4.00 CHF\r\n\r\n- Foulard Grösse: 68x68 cm\r\n- Produktgrösse (verpackt): ?", "weight" => 25, "intern_code" => "DIS A4203", "unit_id" => 1, "sale_price" => "145.000000", "height" => 68, ...}, %{"deliverer_code" => "DIS A4203 (B-Quality)", "currency_id" => 1, "article_group_id" => nil, "html_text" => nil, "stock_available_nr" => 1, "stock_place_id" => nil, "stock_id" => 1, "intern_name" => "Amafu Seidenfoulard, 68x68 (B Quality)", "tax_expense_id" => 22, "user_id" => 1, "stock_min_nr" => 5, "stock_disposed_nr" => 0, "stock_reserved_nr" => 0, "sale_total" => nil, "expense_account_id" => 174, "width" => 68, "stock_nr" => 2, "is_stock" => true, "tax_id" => 43, "stock_picked_nr" => 1, "deliverer_description" => "Druck auf nachhaltige Seide Twill 12MM mit Zertifikat.\r\nRollierte Kanten.\r\nEndformat: 68x68 cm\r\n\r\n68x68 cm fertig konfektioniert:\r\n60 St - € 23,90\r\n100 St - € 18,90\r\n\r\n90x90 cm fertig konfektioniert:\r\n60 St - € 45,00\r\n100 St - € 40,00", "article_type_id" => 1, "tax_income_id" => 43, "stock_ordered_nr" => 0, "account_id" => nil, "id" => 397, "purchase_price" => "17.000000", "deliverer_name" => "Foulard Amafu , ART B12148 GOTS SETA TWILL 12MM, DIS A4203, 70x70", "contact_id" => 4, "purchase_total" => nil, "intern_description" => "<strong>Farbe</strong>: hellblau&nbsp;<strong>Gr&ouml;sse</strong>: 68 x 68 cm <strong>Verarbeitung</strong>: GOTS-Zert. Seide, rollierter Saum <strong>Herkunft</strong>: Designt in der Schweiz, hergestellt in Italien" (truncated)

  @impl true
  def on_update(update_arg, ctx) do
    IO.inspect(%{update_arg: update_arg}, label: "on_update")
    {:ok, ctx}
  end

  @impl true
  def cell_edited(payload, ctx) do
    IO.puts("cell_edited callback in DataTable")

    payload_dump = inspect(payload, label: "Payload", pretty: true)
    ctx_dump = inspect(ctx, label: "Context", pretty: true)

    # Optional: build file-friendly identifiers
    # timestamp = :os.system_time(:millisecond)

    # Write payload
    File.write!("/tmp/livebook_payload.txt", payload_dump)

    # Write ctx
    File.write!("/tmp/livebook_ctx.txt", ctx_dump)

    {:ok, ctx}
  end

  # def handle_event("cell_edited", payload, ctx) do
  #   # def handle_cast("cell_edited", payload) do
  #   IO.inspect(payload, label: "Cell edited payload")
  #   IO.inspect(ctx, label: "Cell edited ctx")

  #   {:noreply, ctx}
  # end

  def handle_event(event, text, ctx) do
    IO.inspect(%{text: text, event: event}, label: "Dummy")
    # broadcast_event(ctx, "update_text", text)
    {:noreply, ctx}
  end

  # def handle_event(event, payload, ctx) do
  #   # def handle_cast("cell_edited", payload) do
  #   IO.inspect(%{event: event, payload: payload, ctx: ctx}, label: "Generic event handler /3")
  #   {:noreply, ctx}
  # end

  # def handle_event(event, ctx) do
  #   # def handle_cast("cell_edited", payload) do
  #   IO.inspect(%{event: event, ctx: ctx}, label: "Generic event handler /2")
  #   {:noreply, ctx}
  # end

  @impl true
  def init(
        {data_rows, data_columns, count, name, sorting_enabled, pagination_enabled, edit_callback,
         inspected, formatter, num_rows}
      ) do
    features = Kino.Utils.truthy_keys(pagination: pagination_enabled, sorting: sorting_enabled)
    info = %{name: name, features: features}
    info = if(num_rows, do: Map.put(info, :num_rows, num_rows), else: info)

    {count, slicing_fun, slicing_cache} = init_slicing(data_rows, count)

    {:ok, info,
     %{
       data_rows: data_rows,
       total_rows: count,
       slicing_fun: slicing_fun,
       slicing_cache: slicing_cache,
       edit_callback: edit_callback,
       columns:
         Enum.map(data_columns, fn key ->
           %{key: key, label: value_to_string(:__header__, key, formatter)}
         end),
       inspected: inspected,
       formatter: formatter
     }}
  end

  defp init_slicing(data_rows, count) do
    {count, slicing_fun} =
      case Enumerable.slice(data_rows) do
        {:ok, count, fun} when is_function(fun, 2) -> {count, fun}
        {:ok, count, fun} when is_function(fun, 3) -> {count, &fun.(&1, &2, 1)}
        _ -> {count, nil}
      end

    if slicing_fun do
      slicing_fun = fn start, length, cache ->
        max_length = max(count - start, 0)
        length = min(length, max_length)
        {slicing_fun.(start, length), count, cache}
      end

      {count, slicing_fun, nil}
    else
      cache = %{items: [], length: 0, continuation: take_init(data_rows)}

      slicing_fun = fn start, length, cache ->
        to_take = start + length - cache.length

        cache =
          if to_take > 0 and cache.continuation != nil do
            {items, length, continuation} = take(cache.continuation, to_take)

            %{
              cache
              | items: cache.items ++ items,
                length: cache.length + length,
                continuation: continuation
            }
          else
            cache
          end

        count = if(cache.continuation, do: count, else: cache.length)

        {Enum.slice(cache.items, start, length), count, cache}
      end

      {count, slicing_fun, cache}
    end
  end

  defp take_init(enumerable) do
    reducer = fn
      x, {acc, 1} ->
        {:suspend, {[x | acc], 0}}

      x, {acc, n} when n > 1 ->
        {:cont, {[x | acc], n - 1}}
    end

    &Enumerable.reduce(enumerable, &1, reducer)
  end

  defp take(continuation, amount) do
    case continuation.({:cont, {[], amount}}) do
      {:suspended, {items, 0}, continuation} ->
        {Enum.reverse(items), amount, continuation}

      {:halted, {items, left}} ->
        {Enum.reverse(items), amount - left, nil}

      {:done, {items, left}} ->
        {Enum.reverse(items), amount - left, nil}
    end
  end

  @impl true
  def get_data(rows_spec, state) do
    {records, count, slicing_cache} =
      query(state.data_rows, state.slicing_fun, state.slicing_cache, rows_spec)

    data =
      Enum.map(records, fn record ->
        Enum.map(state.columns, fn column ->
          value_to_string(column.key, Map.fetch!(record, column.key), state.formatter)
        end)
      end)

    total_rows = count || state.total_rows

    {:ok,
     %{
       columns: state.columns,
       data: {:rows, data},
       total_rows: total_rows
     }, %{state | total_rows: total_rows, slicing_cache: slicing_cache}}
  end

  defp query(data, slicing_fun, slicing_cache, rows_spec) do
    if order = rows_spec[:order] do
      sorted = Enum.sort_by(data, & &1[order.key], order.direction)
      records = Enum.slice(sorted, rows_spec.offset, rows_spec.limit)
      {records, Enum.count(sorted), slicing_cache}
    else
      slicing_fun.(rows_spec.offset, rows_spec.limit, slicing_cache)
    end
  end

  defp value_to_string(_key, value, nil) do
    value_to_string(value)
  end

  defp value_to_string(key, value, formatter) do
    case formatter.(key, value) do
      {:ok, string} -> string
      :default -> value_to_string(value)
    end
  end

  defp value_to_string(value) when is_atom(value), do: inspect(value)

  defp value_to_string(value) when is_list(value) do
    if List.ascii_printable?(value) do
      List.to_string(value)
    else
      inspect(value)
    end
  end

  defp value_to_string(value) when is_binary(value) do
    inspect_opts = Inspect.Opts.new([])

    if String.printable?(value, inspect_opts.limit) do
      value
    else
      inspect(value)
    end
  end

  defp value_to_string(value) do
    if mod = String.Chars.impl_for(value) do
      apply(mod, :to_string, [value])
    else
      inspect(value)
    end
  end

  @impl true
  def on_update({data_rows, data_columns, count, inspected}, state) do
    {count, slicing_fun, slicing_cache} = init_slicing(data_rows, count)

    {:ok,
     %{
       state
       | data_rows: data_rows,
         total_rows: count,
         slicing_fun: slicing_fun,
         slicing_cache: slicing_cache,
         columns:
           Enum.map(data_columns, fn key ->
             %{key: key, label: value_to_string(:__header__, key, state.formatter)}
           end),
         inspected: inspected
     }}
  end
end
