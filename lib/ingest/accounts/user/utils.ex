defmodule Ingest.Accounts.User.Utils do
  @moduledoc """
  This module contains user predicates for permission evaluations.
  """
  alias Ingest.Accounts.User
  alias Ingest.Repo

  def user_may_update_project_role?(user, project) do
    roles = for project_role <- (
      Repo.get(User, user.id)
      |> Repo.preload(:project_roles)
    ).project_roles, into: %{} do
      {project_role.project_id, project_role.role}
    end
    user.roles == :admin or roles[project.id] == :owner
  end
end
