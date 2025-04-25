defmodule Ingest.Uploaders.Lakefs do
  @moduledoc """
  Used for uploading to LakeFS repositories. Typically a request will open a branch
  specifically for the user doing the upload, can be traced all the way back.
  """
  alias Ingest.Accounts.User
  alias Ingest.Requests.Request
  alias Ingest.Destinations.LakeFSConfig
  alias Ingest.Destinations.Destination
  require Logger

  def init!(%Destination{} = destination, filename, state, opts \\ []) do
    original_filename = Keyword.get(opts, :original_filename, nil)

    Logger.info("INIT DESTINATION- #{inspect(destination)}")
    Logger.info("INIT state- #{inspect(state)}")
    Logger.info("INIT FILENAME- #{inspect(filename)}")

    case upsert_branch(destination, state.request, state.user) do
      {:ok, branch_name} ->
        repository =
          if destination.additional_config do
            destination.additional_config["repository_name"]
          else
            destination.lakefs_config.repository
          end

        # Check if object already exists to maybe rename it
        filename =
          with s3_op <- ExAws.S3.head_object("#{repository}/#{branch_name}", filename),
               s3_config <- build_config(destination.lakefs_config),
               {:ok, _res} <- ExAws.request(s3_op, s3_config) do
            "#{filename} - COPY #{DateTime.utc_now() |> DateTime.to_naive()}"
          else
            _ -> filename
          end

        # Add original filename prefix if needed
        filename =
          if original_filename do
            "#{original_filename} Supporting Data/ #{filename}"
          else
            filename
          end

        with s3_op <-
               ExAws.S3.initiate_multipart_upload("#{repository}/#{branch_name}", filename),
             s3_config <- build_config(destination.lakefs_config),
             {:ok, %{body: %{upload_id: upload_id}}} <- ExAws.request(s3_op, s3_config) do
          {:ok,
           {destination,
            state
            |> Map.put(:filename, filename)
            |> Map.put(:chunk, 1)
            |> Map.put(:config, s3_config)
            |> Map.put(:op, s3_op |> Map.put(:upload_id, upload_id) |> Map.put(:opts, []))
            |> Map.put(:upload_id, upload_id)
            |> Map.put(:parts, [])}}
        else
          err ->
            Logger.error("LakeFS upload init error: #{inspect(err)}")
            {:error, err}
        end

      {:error, reason} ->
        Logger.error("Could not upsert branch: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def upload_full_object(
        %Destination{} = destination,
        %Request{} = request,
        %User{} = user,
        filename,
        data
      ) do
    case upsert_branch(destination, request, user) do
      {:ok, branch_name} ->
        repository =
          if destination.additional_config do
            destination.additional_config["repository_name"]
          else
            destination.lakefs_config.repository
          end

        s3_op = ExAws.S3.put_object("#{repository}/#{branch_name}", filename, data)
        s3_config = build_config(destination.lakefs_config)

        case ExAws.request(s3_op, s3_config) do
          {:ok, %{status_code: 200}} ->
            {:ok, :uploaded}

          {:ok, other} ->
            {:error, {:unexpected_success_response, other}}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("Failed to upsert branch for upload: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def update_metadata(
        %Destination{} = destination,
        %Request{} = request,
        %User{} = user,
        filename,
        data
      ) do
    case upsert_branch(destination, request, user) do
      {:ok, branch_name} ->
        repository =
          if destination.additional_config do
            destination.additional_config["repository_name"]
          else
            destination.lakefs_config.repository
          end

        s3_op =
          ExAws.S3.put_object_copy(
            "#{repository}/#{branch_name}",
            filename,
            "#{repository}/#{branch_name}",
            filename,
            [
              {:metadata_directive, "REPLACE"},
              {:meta, data}
            ]
          )

        s3_config = build_config(destination.lakefs_config)

        case ExAws.request(s3_op, s3_config) do
          {:ok, %{status_code: 200}} ->
            {:ok, :metadata_updated}

          {:ok, other} ->
            {:error, {:unexpected_success_response, other}}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("Failed to upsert branch for metadata update: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def upload_chunk(%Destination{} = destination, _filename, state, data, _opts \\ []) do
    part = ExAws.S3.Upload.upload_chunk({data, state.chunk}, state.op, state.config)

    case part do
      {:error, err} -> {:error, err}
      _ -> {:ok, {destination, %{state | chunk: state.chunk + 1, parts: [part | state.parts]}}}
    end
  end

  def commit(%Destination{} = destination, _filename, state, _opts \\ []) do
    result = ExAws.S3.Upload.complete(state.parts, state.op, state.config)

    case result do
      {:ok, %{body: %{key: _key}}} ->
        {:ok, {destination, state.filename}}

      _ ->
        {:error, result}
    end
  end

  defp build_config(%LakeFSConfig{} = config) do
    ExAws.Config.new(:s3,
      host: Map.get(config, :base_url, nil),
      scheme:
        if Map.get(config, :ssl, true) do
          "https://"
        else
          "http://"
        end,
      port: config.port,
      access_key_id: config.access_key_id,
      secret_access_key: config.secret_access_key,
      ex_aws: [
        access_key_id: config.access_key_id,
        secret_access_key: config.secret_access_key,
        region: Map.get(config, config.region, "us-east-1")
      ]
    )
  end

  # defp upsert_branch(%Destination{} = destination, %Request{} = request, %User{} = user) do
  #   branch_name = Regex.replace(~r/\W+/, "#{request.name}-by-#{user.name}", "-")

  #   config = destination.lakefs_config

  #   repository =
  #     destination.additional_config["repository_name"] || config.repository

  #   client =
  #     Ingest.LakeFS.new!(
  #       %URI{
  #         host: config.base_url,
  #         scheme: if(config.ssl, do: "https", else: "http"),
  #         port: config.port
  #       },
  #       access_key: config.access_key_id,
  #       secret_access_key: config.secret_access_key
  #     )

  #   case Ingest.LakeFS.list_branches(client, repository) do
  #     {:ok, branches} ->
  #       branch_exists? = Enum.any?(branches, fn b -> b["id"] == branch_name end)

  #       if branch_exists? do
  #         {:ok, branch_name}
  #       else
  #         case Ingest.LakeFS.create_branch(client, repository, branch_name) do
  #           {:ok, _res} ->
  #             Logger.info("Created branch #{branch_name} in #{repository}")
  #             {:ok, branch_name}

  #           {:error, :precondition_failed} ->
  #             Logger.warn("Branch already exists or conflict: #{branch_name}")
  #             {:ok, branch_name}

  #           {:error, reason} ->
  #             Logger.error("Failed to create branch #{branch_name}: #{inspect(reason)}")
  #             {:error, reason}
  #         end
  #       end

  #     {:error, reason} ->
  #       Logger.error("Failed to list branches for #{repository}: #{inspect(reason)}")
  #       nil
  #   end
  # end
  defp upsert_branch(%Destination{} = destination, %Request{} = request, %User{} = user) do
    branch_name = Regex.replace(~r/\W+/, "#{request.name}-by-#{user.name}", "-")

    config = destination.lakefs_config

    repository =
      destination.additional_config["repository_name"] || config.repository

    client =
      Ingest.LakeFS.new!(
        %URI{
          host: config.base_url,
          scheme: if(config.ssl, do: "https", else: "http"),
          port: config.port
        },
        access_key: config.access_key_id,
        secret_access_key: config.secret_access_key,
        storage_namespace: config.storage_namespace
      )

    # Ensure repository exists
    case Ingest.LakeFS.get_repo(client, repository) do
      {:ok, _repo} ->
        :ok

      {:error, :not_found} ->
        Logger.warn("Repository #{repository} not found, attempting to create it...")

        case Ingest.LakeFS.create_repo(client, repository,
               storage_namespace: config.storage_namespace
             ) do
          {:ok, _} ->
            Logger.info("Created repository #{repository}")
            :ok

          {:error, reason} ->
            Logger.error("Could not create repository #{repository}: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("Failed to check repository #{repository}: #{inspect(reason)}")
        {:error, reason}
    end
    |> case do
      :ok ->
        # Proceed to check/create branch
        case Ingest.LakeFS.list_branches(client, repository) do
          {:ok, branches} ->
            branch_exists? = Enum.any?(branches, fn b -> b["id"] == branch_name end)

            if branch_exists? do
              {:ok, branch_name}
            else
              case Ingest.LakeFS.create_branch(client, repository, branch_name) do
                {:ok, _res} ->
                  Logger.info("Created branch #{branch_name} in #{repository}")
                  {:ok, branch_name}

                {:error, :precondition_failed} ->
                  Logger.warn("Branch already exists or conflict: #{branch_name}")
                  {:ok, branch_name}

                {:error, reason} ->
                  Logger.error("Failed to create branch #{branch_name}: #{inspect(reason)}")
                  {:error, reason}
              end
            end

          {:error, reason} ->
            Logger.error("Failed to list branches for #{repository}: #{inspect(reason)}")
            {:error, reason}
        end

      other ->
        other
    end
  end
end
