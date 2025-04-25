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

    case upsert_branch(destination, state.request, state.user) do
      {:ok, branch_name} ->
        repository =
          if destination.additional_config do
            destination.additional_config["repository_name"]
          else
            destination.lakefs_config.repository
          end

        filename =
          case ExAws.request(
                 ExAws.S3.head_object("#{repository}/#{branch_name}", filename),
                 build_config(destination.lakefs_config)
               ) do
            {:ok, _res} ->
              "#{filename} - COPY #{DateTime.utc_now() |> DateTime.to_naive()}"

            {:error, {:http_error, 404, _}} ->
              # Not found? use the original filename
              filename

            {:error, reason} ->
              Logger.warn(
                "[LakeFS INIT] Unexpected error checking file existence: #{inspect(reason)}"
              )

              filename
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

      # END OF FUNCTIONS INTERNAL TO UPSERT BRANCH CASE
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
    Logger.info("UPLOAD FULL OBJECT FIRING")

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

  defp upsert_branch(%Destination{} = destination, %Request{} = request, %User{} = user) do
    branch_name = Regex.replace(~r/\W+/, "#{request.name}-by-#{user.name}", "-")

    repo_name =
      destination.additional_config["repository_name"] ||
        destination.lakefs_config.repository

    config = destination.lakefs_config

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

    Logger.info("[LakeFS Upsert] Working on repo #{repo_name} with branch #{branch_name}")

    # check repo exists
    with {:ok, _repo} <- ensure_repo_exists(client, repo_name, config.storage_namespace),
         {:ok, branch_name} <- ensure_branch_exists(client, repo_name, branch_name) do
      {:ok, branch_name}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_repo_exists(client, repo_name, storage_namespace) do
    case Ingest.LakeFS.get_repo(client, repo_name) do
      {:ok, _repo} ->
        Logger.debug("[LakeFS Upsert] Repo #{repo_name} already exists")
        {:ok, :already_exists}

      {:error, :not_found} ->
        Logger.warn("[LakeFS Upsert] Repo #{repo_name} not found, creating...")

        case Ingest.LakeFS.create_repo(client, repo_name, storage_namespace: storage_namespace) do
          {:ok, _} ->
            Logger.info("[LakeFS Upsert] Repo #{repo_name} created successfully")
            {:ok, :created}

          {:error, reason} ->
            Logger.error("[LakeFS Upsert] Failed to create repo #{repo_name}: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("[LakeFS Upsert] Failed to check repo #{repo_name}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp ensure_branch_exists(client, repo_name, branch_name) do
    case Ingest.LakeFS.list_branches(client, repo_name) do
      {:ok, branches} ->
        branch_exists? = Enum.any?(branches, fn b -> b["id"] == branch_name end)

        if branch_exists? do
          Logger.debug(
            "[LakeFS Upsert] Branch #{branch_name} already exists in repo #{repo_name}"
          )

          {:ok, branch_name}
        else
          case Ingest.LakeFS.create_branch(client, repo_name, branch_name) do
            {:ok, _res} ->
              Logger.info("[LakeFS Upsert] Created branch #{branch_name} in #{repo_name}")
              {:ok, branch_name}

            {:error, :precondition_failed} ->
              Logger.warn(
                "[LakeFS Upsert] Branch already existed (precondition failed): #{branch_name}"
              )

              {:ok, branch_name}

            {:error, reason} ->
              Logger.error(
                "[LakeFS Upsert] Failed to create branch #{branch_name}: #{inspect(reason)}"
              )

              {:error, reason}
          end
        end

      {:error, reason} ->
        Logger.error(
          "[LakeFS Upsert] Failed to list branches for #{repo_name}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

end
